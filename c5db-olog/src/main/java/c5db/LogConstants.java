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

public class LogConstants {
  public static final String LOG_ROOT_DIRECTORY_NAME = "logs";
  public static final String LOG_FILE_SUBDIRECTORY_NAME = "files";
  public static final int LOG_THREAD_POOL_SIZE = 1;
  public static final int LOG_CLOSE_TIMEOUT_SECONDS = 15;
  public static final int LOG_NAVIGATOR_DEFAULT_MAX_ENTRY_SEEK = 256;
  public static final boolean LOG_USE_FILE_CHANNEL_FORCE = true;
}
