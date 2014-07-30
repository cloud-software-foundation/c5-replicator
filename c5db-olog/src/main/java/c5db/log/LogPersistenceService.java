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

import c5db.interfaces.log.SequentialEntryCodec;
import c5db.util.CheckedSupplier;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import static c5db.log.SequentialLog.LogEntryNotFound;

/**
 * Service to handle persistence for different quorums' logs. Each quorum's log is
 * represented by a list of persisted data stores. Apart from administrative actions,
 * such as deleting old and unneeded log records, stores may only be added to or
 * removed from the end of the list. At any given time, the "current" data store
 * is the most recent one, at the end of the list. Data stores are accessed and
 * manipulated via returned BytePersistence instances.
 */
public interface LogPersistenceService<P extends LogPersistenceService.BytePersistence> {
  /**
   * Return a persistence instance referring to the latest (most recent) data
   * store persisted for the given quorum. If there is none, return null.
   *
   * @param quorumId ID of the quorum.
   * @return A new, open BytePersistence pointing to the latest log data store
   * for the given quorum, or null if there is none.
   * @throws IOException
   */
  @Nullable
  P getCurrent(String quorumId) throws IOException;

  /**
   * Create a new, empty data store, and return an object representing it. The store
   * is temporary; if not appended to the log, it will not persist.
   *
   * @param quorumId ID of the quorum.
   * @return A new, open persistence instance.
   * @throws IOException
   */
  @NotNull
  P create(String quorumId) throws IOException;

  /**
   * Atomically add the given persistence to the log for the given quorum. After this
   * method call, getCurrentPersistence will return an instance referring to the same
   * underlying data store as the given instance (but not necessarily the same instance).
   *
   * @param quorumId    Quorum ID.
   * @param persistence The persistence to append to the log, making it current.
   * @throws IOException
   */
  void append(String quorumId, @NotNull P persistence) throws IOException;

  /**
   * Atomically remove and delete the data store underlying the "current" persistence
   * for the given quorum, in effect truncating its contents from the log record, and
   * rendering any instances referring to this data store invalid.
   *
   * @param quorumId Quorum ID.
   * @throws IOException
   */
  void truncate(String quorumId) throws IOException;

  /**
   * Return an list of the data stores for this quorum, in order from most recent
   * to least recent. This method may perform IO, to determine which stores are
   * present. Rather than the list directly containing persistence objects, it contains
   * Suppliers that return them when invoked; that way additional IO is only performed
   * if needed.
   * <p>
   * The first element (index 0) returned will be a Supplier that returns the element
   * returned by getCurrent. The user is responsible for closing any BytePersistence
   * objects returned by the Suppliers.
   *
   * @return A new ImmutableList.
   */
  ImmutableList<CheckedSupplier<P, IOException>> getList(String quorumId) throws IOException;

  /**
   * Represents a single store of persisted log data; a file-like abstraction.
   */
  interface BytePersistence extends AutoCloseable {
    /**
     * Determine if the store is empty.
     *
     * @return True if empty, otherwise false.
     * @throws IOException
     */
    boolean isEmpty() throws IOException;

    /**
     * Get the size in bytes of the data, equal to the position/address of
     * next byte to be appended.
     *
     * @return Size in bytes.
     * @throws IOException
     */
    long size() throws IOException;

    /**
     * Append data.
     *
     * @param buffers Data to append.
     *                IOException if the persistence is closed, or if the underlying object is inaccessible.
     */
    void append(ByteBuffer[] buffers) throws IOException;

    /**
     * Get a new reader of the data. Each reader is independent of any other,
     * and the caller takes responsibility for releasing associated resources.
     *
     * @return A new reader instance.
     * @throws IOException
     */
    PersistenceReader getReader() throws IOException;

    /**
     * Truncate data from the end, to a certain size.
     *
     * @param size New size of the data, equal to the position/address of the next
     *             byte to be appended after the truncation. After calling this method,
     *             the size() method will return this value of size.
     * @throws IOException if the persistence is closed, or if the underlying object is inaccessible.
     */
    void truncate(long size) throws IOException;

    /**
     * Sync previous operations to the underlying medium.
     *
     * @throws IOException if the persistence is closed, or if the underlying object is inaccessible.
     */
    void sync() throws IOException;

    /**
     * Release held resources.
     *
     * @throws IOException
     */
    void close() throws IOException;
  }

  /**
   * Seekable reader of a BytePersistence that keeps track of its own position within
   * the data.
   */
  interface PersistenceReader extends ReadableByteChannel, AutoCloseable {
    long position() throws IOException;

    void position(long newPos) throws IOException;
  }

  /**
   * Service in charge of finding entries in a persistence by their sequence number, rather
   * than by their byte position/address. To facilitate fast navigation, it may build an index
   * of already-written entries and their locations.
   */
  interface PersistenceNavigator {
    /**
     * Updates the navigator with information about an entry to be written, so that it can
     * optionally incorporate that information into an internal index.
     *
     * @param seqNum      Sequence number of an entry being written.
     * @param byteAddress Byte address of the start of the entry within the persistence.
     * @throws IOException
     */
    void notifyLogging(long seqNum, long byteAddress) throws IOException;

    /**
     * Updates the navigator with information about the location of an entry within the
     * persistence, guaranteeing it will be added to the internal index.
     *
     * @param seqNum      Sequence number the entry.
     * @param byteAddress Byte address of the start of the entry within the persistence.
     * @throws IOException
     */
    void addToIndex(long seqNum, long byteAddress) throws IOException;

    /**
     * Updates the navigator that a truncation is being performed on the log, which may
     * necessitate updating the navigator's internal index.
     *
     * @param seqNum Sequence number of the start of the truncation; i.e., entries greater than
     *               or equal to this sequence number are being deleted.
     * @throws IOException
     */
    void notifyTruncation(long seqNum) throws IOException;

    /**
     * Find the position or address of an entry within the persistence, using its sequence number.
     *
     * @param seqNum Sequence number of entry to find.
     * @return Byte position or address.
     * @throws IOException, LogEntryNotFound
     */
    long getAddressOfEntry(long seqNum) throws IOException, LogEntryNotFound;

    /**
     * Return an input stream ready to read from the persistence starting at the beginning of the
     * entry with the specified sequence number.
     *
     * @param fromSeqNum Sequence number to read from.
     * @return A new input stream; the caller takes responsibility for closing it.
     * @throws IOException, LogEntryNotFound
     */
    InputStream getStreamAtSeqNum(long fromSeqNum) throws IOException, LogEntryNotFound;

    /**
     * Return an input stream ready to read from the persistence starting at the beginning of the
     * first entry in the persistence. If there are no entries, the stream will be positioned at
     * the end of the persistence.
     *
     * @return A new input stream; the caller takes responsibility for closing it.
     * @throws IOException
     */
    InputStream getStreamAtFirstEntry() throws IOException;

    /**
     * Return an input stream ready to read from the persistence starting at the beginning of the
     * last entry in the persistence. Throws an unchecked exception if the log is empty.
     *
     * @return A new input stream; the caller takes responsibility for closing it.
     * @throws IOException
     */
    InputStream getStreamAtLastEntry() throws IOException;
  }

  interface PersistenceNavigatorFactory {
    PersistenceNavigator create(BytePersistence persistence, SequentialEntryCodec<?> encoding, long offset);
  }
}
