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
