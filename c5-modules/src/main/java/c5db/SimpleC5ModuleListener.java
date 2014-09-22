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

package c5db;

import c5db.interfaces.C5Module;
import com.google.common.util.concurrent.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * A {@link com.google.common.util.concurrent.Service.Listener} that logs the module lifecycle
 * and performs a single action when the module stops or fails.
 */
public class SimpleC5ModuleListener implements Service.Listener {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final C5Module module;
  private final Runnable onRunningModule;
  private final Runnable onStoppingModule;
  private final Consumer<Throwable> throwableConsumer;

  public SimpleC5ModuleListener(C5Module module,
                                Runnable onRunningModule,
                                Runnable onStoppingModule,
                                Consumer<Throwable> throwableConsumer) {
    this.module = module;
    this.onRunningModule = onRunningModule;
    this.onStoppingModule = onStoppingModule;
    this.throwableConsumer = throwableConsumer;
  }

  @Override
  public void starting() {
    logger.info("Started module {}", module);
  }

  @Override
  public void running() {
    logger.info("Running module {}", module);
    onRunningModule.run();
  }

  @Override
  public void stopping(Service.State from) {
    logger.info("Stopping module {}", module);
    onStoppingModule.run();
  }

  @Override
  public void terminated(Service.State from) {
    logger.info("Terminated module {}", module);
    onStoppingModule.run();
  }

  @Override
  public void failed(Service.State from, Throwable failure) {
    logger.error("Failed module {}: {}", module, failure);
    onStoppingModule.run();
    throwableConsumer.accept(failure);
  }
}
