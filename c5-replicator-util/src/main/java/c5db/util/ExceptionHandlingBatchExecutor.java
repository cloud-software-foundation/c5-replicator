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

import org.jetlang.core.BatchExecutor;
import org.jetlang.core.EventReader;

/**
 * BatchExecutor implementation that catches Throwables (including RuntimeExceptions, etc), and calls the
 * provided Consumer<Throwable> function for flexible error handling.  The default implementation does not
 * catch any exceptions and can result in thread death, causing havoc.
 */
public class ExceptionHandlingBatchExecutor implements BatchExecutor {
  private final Consumer<Throwable> handler;

  /**
   * Create a batch executor with the specified Throwable handler. In the event that the executing fiber
   * throws an uncaught, unchecked exception, the handler will be passed that exception. The handler executes
   * in the context of the failed fiber, so it can/should take remedial action immediately if necessary.
   *
   * @param handler Consumer to accept Throwables thrown by fibers using this batch executor
   */
  public ExceptionHandlingBatchExecutor(Consumer<Throwable> handler) {
    this.handler = handler;
  }

  @Override
  public void execute(EventReader toExecute) {
    for (int i = 0; i < toExecute.size(); i++) {
      try {
        toExecute.get(i).run();
      } catch (Throwable throwable) {
        handler.accept(throwable);
      }
    }
  }
}
