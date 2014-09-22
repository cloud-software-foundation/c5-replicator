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
