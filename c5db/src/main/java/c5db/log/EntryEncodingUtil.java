/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package c5db.log;

import c5db.util.CrcInputStream;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.protostuff.LinkBuffer;
import io.protostuff.LowCopyProtobufOutput;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.Adler32;

import static com.google.common.math.IntMath.checkedAdd;

/**
 * Contains methods used for encoding and decoding WAL entries
 */
public class EntryEncodingUtil {

  /**
   * Exception indicating that a CRC has been read which does not match up with
   * the CRC computed from the associated data.
   */
  public static class CrcError extends RuntimeException {
    public CrcError(String s) {
      super(s);
    }
  }

  /**
   * Serialize a protostuff message object, prefixed with message length, and suffixed with a 4-byte CRC.
   *
   * @param schema  Protostuff message schema
   * @param message Object to serialize
   * @param <T>     Message type
   * @return A list of ByteBuffers containing a varInt length, followed by the message, followed by a 4-byte CRC.
   */
  public static <T> List<ByteBuffer> encodeWithLengthAndCrc(Schema<T> schema, T message) {
    final LinkBuffer messageBuf = new LinkBuffer();
    final LowCopyProtobufOutput lcpo = new LowCopyProtobufOutput(messageBuf);

    try {
      schema.writeTo(lcpo, message);

      final int length = (int) lcpo.buffer.size();
      final LinkBuffer lengthBuf = new LinkBuffer().writeVarInt32(length);

      return appendCrcToBufferList(
          Lists.newArrayList(
              Iterables.concat(lengthBuf.finish(), messageBuf.finish())));
    } catch (IOException e) {
      // This method performs no IO, so it should not actually be possible for an IOException to be thrown.
      // But just in case...
      throw new RuntimeException(e);
    }
  }

  /**
   * Decode a message from the passed input stream, and compute and verify its CRC. This method reads
   * data written by the method {@link EntryEncodingUtil#encodeWithLengthAndCrc}.
   *
   * @param inputStream Input stream, opened for reading and positioned just before the length-prepended header
   * @return The deserialized, constructed, validated message
   * @throws IOException                if a problem is encountered while reading or parsing
   * @throws EntryEncodingUtil.CrcError if the recorded CRC of the message does not match its computed CRC.
   */
  public static <T> T decodeAndCheckCrc(InputStream inputStream, Schema<T> schema)
      throws IOException, CrcError {
    // TODO this should check the length first and compare it with a passed-in maximum length
    final T message = schema.newMessage();
    final CrcInputStream crcStream = new CrcInputStream(inputStream, new Adler32());
    ProtostuffIOUtil.mergeDelimitedFrom(crcStream, message, schema);

    final long computedCrc = crcStream.getValue();
    final long diskCrc = readCrc(inputStream);
    if (diskCrc != computedCrc) {
      throw new CrcError("CRC mismatch on deserialized message " + message.toString());
    }

    return message;
  }

  /**
   * Given a list of ByteBuffers, compute the combined CRC and then append it to the list as one or more
   * additional ByteBuffers. Return the entire resulting collection as a new list, including the original
   * ByteBuffers.
   *
   * @param content non-null list of ByteBuffers; no mutation will be performed on them.
   * @return New list of ByteBuffers, with the CRC appended to the original ByteBuffers
   */
  public static List<ByteBuffer> appendCrcToBufferList(List<ByteBuffer> content) throws IOException {
    assert content != null;

    final Adler32 crc = new Adler32();
    content.forEach((ByteBuffer buffer) -> crc.update(buffer.duplicate()));
    final LinkBuffer crcBuf = new LinkBuffer(8);
    putCrc(crcBuf, crc.getValue());

    return Lists.newArrayList(Iterables.concat(content, crcBuf.finish()));
  }

  /**
   * Write a passed CRC to the passed buffer. The CRC is a 4-byte unsigned integer stored in a long; write it
   * as (fixed length) 4 bytes.
   *
   * @param writeTo Buffer to write to; exactly 4 bytes will be written.
   * @param crc     CRC to write; caller guarantees that the code is within the range:
   *                0 <= CRC < 2^32
   */
  private static void putCrc(final LinkBuffer writeTo, final long crc) throws IOException {
    // To store the CRC in an int, we need to subtract to convert it from unsigned to signed.
    final long shiftedCrc = crc + Integer.MIN_VALUE;
    writeTo.writeInt32(Ints.checkedCast(shiftedCrc));
  }

  private static long readCrc(InputStream inputStream) throws IOException {
    int shiftedCrc = (new DataInputStream(inputStream)).readInt();
    return ((long) shiftedCrc) - Integer.MIN_VALUE;
  }

  /**
   * Read a specified number of bytes from the input stream (the "content"), then read one or more CRC codes and
   * check the validity of the data.
   *
   * @param inputStream   Input stream, opened for reading and positioned just before the content
   * @param contentLength Length of data to read from inputStream, not including any trailing CRCs
   * @return The read content, as a ByteBuffer.
   * @throws IOException
   */
  public static ByteBuffer getAndCheckContent(InputStream inputStream, int contentLength)
      throws IOException, CrcError {
    // TODO probably not the correct way to do this... should use IOUtils?
    final CrcInputStream crcStream = new CrcInputStream(inputStream, new Adler32());
    final byte[] content = new byte[contentLength];
    final int len = crcStream.read(content);

    if (len < contentLength) {
      // Data wasn't available that we expected to be
      throw new IllegalStateException("Reading a log entry's contents returned fewer than expected bytes");
    }

    final long computedCrc = crcStream.getValue();
    final long diskCrc = readCrc(inputStream);
    if (diskCrc != computedCrc) {
      throw new CrcError("CRC mismatch on log entry contents");
    }

    return ByteBuffer.wrap(content);
  }

  public static void skip(InputStream inputStream, int numBytes) throws IOException {
    long actuallySkipped = inputStream.skip(numBytes);
    if (actuallySkipped < numBytes) {
      throw new IOException("Unable to skip requested number of bytes");
    }
  }

  /**
   * Add up the lengths of the content of each buffer in the passed list, and return the sum of the lengths.
   *
   * @param buffers List of buffers; this method will not mutate them.
   * @return The sum of the remaining() bytes in each buffer.
   */
  public static int sumLengths(List<ByteBuffer> buffers) {
    int length = 0;
    if (buffers != null) {
      for (ByteBuffer b : buffers) {
        length = checkedAdd(length, b.remaining());
      }
    }
    return length;
  }
}
