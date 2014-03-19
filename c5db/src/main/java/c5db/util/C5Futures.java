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

import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.fibers.Fiber;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * C5 utilities for messing about with guava listenable futures.
 */
public class C5Futures {

  public static <V> void addCallback(ListenableFuture<V> future,
                                     Consumer<? super V> success,
                                     Consumer<Throwable> failure,
                                     Fiber fiber) {
    Runnable callbackListener = () -> {
      final V value;
      try {
        value = getUninterruptibly(future);
      } catch (ExecutionException | RuntimeException | Error e) {
        failure.accept(e);
        return;
      }
      success.accept(value);
    };
    future.addListener(callbackListener, fiber);
  }

  public static <V> V getUninterruptibly(Future<V> future)
      throws ExecutionException {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return future.get();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

}
