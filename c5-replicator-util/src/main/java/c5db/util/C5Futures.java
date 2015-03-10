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

import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.NotNull;
import org.jetlang.fibers.Fiber;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * C5 utilities for messing about with guava listenable futures.
 */
public class C5Futures {

  public static <V> void addCallback(@NotNull final ListenableFuture<V> future,
                                     @NotNull final Consumer<? super V> success,
                                     @NotNull final Consumer<Throwable> failure,
                                     @NotNull Fiber fiber) {
    Runnable callbackListener = new Runnable() {
      @Override
      public void run() {
        final V value;
        try {
          value = getUninterruptibly(future);
        } catch (ExecutionException | RuntimeException | Error e) {
          failure.accept(e);
          return;
        }
        success.accept(value);
      }
    };
    future.addListener(callbackListener, fiber);
  }

  public static <V> V getUninterruptibly(@NotNull Future<V> future)
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
