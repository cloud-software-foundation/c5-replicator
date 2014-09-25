/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package c5db.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 *
 */
public class ByteBufferOutputStream extends OutputStream {
  protected ByteBuffer bb;

  public ByteBufferOutputStream(int size) {
    bb = ByteBuffer.allocate(size);
  }

  public ByteBufferOutputStream(ByteBuffer useMe) {
    bb = useMe.duplicate();
  }

  @Override
  public void write(int b) throws IOException {
    bb.put((byte) b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    bb.put(b, off, len);
  }

  /**
   * Duplicate and flips the internal byte buffer
   *
   * @return a duplicate, and flipped byte buffer (ready for writing elsewhere)
   */
  public ByteBuffer getByteBuffer() {
    ByteBuffer copy = bb.duplicate();
    copy.flip();
    return copy;  // all because flip has the wrong return type
  }

  public void reset() {
    bb.rewind();
  }
}
