/*
 * Copyright (C) 2013  Ohm Data
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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ohmdb.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.FSUtils;

import java.io.IOException;

/**
 * HLogLink describes a link to a WAL.
 *
 * An hlog can be in /hbase/.logs/<server>/<hlog>
 * or it can be in /hbase/.oldlogs/<hlog>
 *
 * The link checks first in the original path,
 * if it is not present it fallbacks to the archived path.
 */
public class HLogLink extends FileLink {
  /**
   * @param conf {@link Configuration} from which to extract specific archive locations
   * @param serverName Region Server owner of the log
   * @param logName WAL file name
   * @throws IOException on unexpected error.
   */
  public HLogLink(final Configuration conf,
      final String serverName, final String logName) throws IOException {
    this(FSUtils.getRootDir(conf), serverName, logName);
  }

  /**
   * @param rootDir Path to the root directory where hbase files are stored
   * @param serverName Region Server owner of the log
   * @param logName WAL file name
   */
  public HLogLink(final Path rootDir, final String serverName, final String logName) {
    final Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    final Path logDir = new Path(new Path(rootDir, HConstants.HREGION_LOGDIR_NAME), serverName);
    setLocations(new Path(logDir, logName), new Path(oldLogDir, logName));
  }

  /**
   * @param originPath Path to the wal in the log directory
   * @param archivePath Path to the wal in the archived log directory
   */
  public HLogLink(final Path originPath, final Path archivePath) {
    setLocations(originPath, archivePath);
  }
}
