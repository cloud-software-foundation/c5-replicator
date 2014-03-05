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

import org.jetlang.core.BatchExecutor;
import org.jetlang.core.EventReader;

import java.util.function.Consumer;

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
