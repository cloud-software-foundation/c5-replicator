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
        bb.put((byte)b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        bb.put(b, off, len);
    }

    /**
     * Duplicate and flips the internal byte buffer
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
