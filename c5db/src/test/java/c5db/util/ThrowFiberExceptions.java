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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * JUnit TestRule intended to be used as a callback for an instance of {@link ExceptionHandlingBatchExecutor},
 * which in turns allows creating fibers such that uncaught exceptions are gathered by this rule. After a test
 * method runs, this rule will rethrow any caught exceptions. They are rethrown in the context of the thread
 * in which the test runs.
 */
public class ThrowFiberExceptions implements TestRule, Consumer<Throwable> {
  private final Object target;
  private final List<Throwable> throwables = new ArrayList<>();

  public ThrowFiberExceptions(Object target) {
    this.target = target;
  }

  @Override
  public void accept(Throwable throwable) {
    throwables.add(throwable);
  }

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          base.evaluate();
        } catch (Throwable t) {
          throwables.add(t);
        }
        MultipleFailureException.assertEmpty(throwables);
      }
    };
  }
}
