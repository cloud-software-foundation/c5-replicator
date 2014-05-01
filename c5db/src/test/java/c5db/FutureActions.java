package c5db;/*
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

import com.google.common.util.concurrent.SettableFuture;
import org.hamcrest.Description;
import org.jmock.api.Action;
import org.jmock.api.Invocation;

/**
 * JMock2 actions for returning futures that have values or exceptions.  Can really cut down on the
 * code by not having to create intermediate futures everywhere.
 */
public class FutureActions {
  public static Action returnFutureWithValue(Object futureValue) {
    return new ReturnFutureWithValueAction(futureValue);
  }
  public static Action returnFutureWithException(Throwable exception) {
    return new ReturnFutureWithException(exception);
  }

  public static class ReturnFutureWithValueAction implements Action {
    private Object futureResult;
    public ReturnFutureWithValueAction(Object futureResult) {
      this.futureResult = futureResult;
    }

    public Object invoke(Invocation invocation) {
      SettableFuture<Object> future = SettableFuture.create();
      future.set(futureResult);
      return future;
    }
    public void describeTo(Description description) {
      description.appendText("returns a future with value ");
      description.appendValue(futureResult);
    }
  }

  public static class ReturnFutureWithException implements Action {
    private Throwable exception;
    public ReturnFutureWithException(Throwable exception) {
      this.exception = exception;
    }

    public Object invoke(Invocation invocation) {
      SettableFuture<Object> future = SettableFuture.create();
      future.setException(exception);
      return future;
    }
    public void describeTo(Description description) {
      description.appendText("returns a future with exception ");
      description.appendValue(exception);
    }
  }


}
