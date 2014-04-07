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
import org.jetlang.fibers.PoolFiberFactory;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Test;

public class PoolFiberFactoryWithExecutorTest {
  private final Mockery context = new Mockery() {{
    setImposteriser(ClassImposteriser.INSTANCE);
  }};
  private final PoolFiberFactory poolFiberFactory = context.mock(PoolFiberFactory.class);
  private final BatchExecutor batchExecutor = context.mock(BatchExecutor.class);

  @Test
  public void createsFibersUsingThePassedPoolFiberFactoryAndBatchExecutor() {
    final PoolFiberFactoryWithExecutor testInstance = new PoolFiberFactoryWithExecutor(poolFiberFactory, batchExecutor);

    context.checking(new Expectations() {{
      oneOf(poolFiberFactory).create(batchExecutor);
    }});

    testInstance.create();
    context.assertIsSatisfied();
  }
}
