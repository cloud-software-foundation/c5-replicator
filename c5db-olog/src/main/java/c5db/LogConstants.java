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

import java.nio.file.Path;
import java.nio.file.Paths;

public class LogConstants {
  public static final Path LOG_ROOT_DIRECTORY_RELATIVE_PATH = Paths.get("logs");
  public static final Path LOG_FILE_SUBDIRECTORY_RELATIVE_PATH = Paths.get("files");
  public static final int LOG_THREAD_POOL_SIZE = 1;
  public static final int LOG_CLOSE_TIMEOUT_SECONDS = 15;
  public static final int LOG_NAVIGATOR_DEFAULT_MAX_ENTRY_SEEK = 256;
  public static final boolean LOG_USE_FILE_CHANNEL_FORCE = true;
}
