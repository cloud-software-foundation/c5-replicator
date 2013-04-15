/*
 * Copyright (C) 2013  Ohm Data
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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ohmdb.io;


import java.io.IOException;
import java.io.OutputStream;

/**
 * An output stream that writes to two streams on each operation. Does not
 * attempt to handle exceptions gracefully. If any operation other than
 * {@link #close()} fails on the first stream, it is not called on the second
 * stream.
 */
public class DoubleOutputStream extends OutputStream {
  private OutputStream out1;
  private OutputStream out2;

  public DoubleOutputStream(OutputStream out1, OutputStream out2) {
    this.out1 = out1;
    this.out2 = out2;
  }

  @Override
  public void write(int b) throws IOException {
    out1.write(b);
    out2.write(b);
  }

  @Override
  public void write(byte b[]) throws IOException {
    out1.write(b, 0, b.length);
    out2.write(b, 0, b.length);
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    out1.write(b, off, len);
    out2.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    out1.flush();
    out2.flush();
  }

  @Override
  public void close() throws IOException {
    try {
      out1.close();
    } finally {
      // Make sure we at least attempt to close both streams.
      out2.close();
    }
  }

}
