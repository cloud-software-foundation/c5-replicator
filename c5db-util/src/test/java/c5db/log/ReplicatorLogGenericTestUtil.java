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

import java.nio.ByteBuffer;
import java.util.Random;

public class ReplicatorLogGenericTestUtil {
  private static final Random deterministicDataSequence = new Random(112233);

  public static long seqNum(long seqNum) {
    return seqNum;
  }

  public static long term(long term) {
    return term;
  }

  public static ByteBuffer someData() {
    byte[] bytes = new byte[10];
    deterministicDataSequence.nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }

  public static ByteBuffer lotsOfData() {
    byte[] bytes = new byte[150];
    deterministicDataSequence.nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }

  public static long anElectionTerm() {
    return Math.abs(deterministicDataSequence.nextLong());
  }

  public static long aSeqNum() {
    return Math.abs(deterministicDataSequence.nextLong());
  }
}
