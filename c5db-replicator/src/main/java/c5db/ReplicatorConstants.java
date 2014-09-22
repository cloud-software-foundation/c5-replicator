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

public class ReplicatorConstants {
  public static final int REPLICATOR_PORT_MIN = 1024;

  public static final Path REPLICATOR_QUORUM_FILE_ROOT_DIRECTORY_RELATIVE_PATH = Paths.get("repl");
  public static final String REPLICATOR_PERSISTER_FILE_NAME = "replication-data";
  public static final int REPLICATOR_MAXIMUM_SIMULTANEOUS_LOG_REQUESTS = 10000;

  public static final int REPLICATOR_NODE_INFO_REQUEST_TIMEOUT_MILLISECONDS = 3000;
  public static final int REPLICATOR_DEFAULT_BASE_ELECTION_TIMEOUT_MILLISECONDS = 1000;
  public static final int REPLICATOR_DEFAULT_ELECTION_CHECK_INTERVAL_MILLISECONDS = 100;
  public static final int REPLICATOR_DEFAULT_LEADER_LOG_INTERVAL_MILLISECONDS = 100;
  public static final int REPLICATOR_VOTE_RPC_TIMEOUT_MILLISECONDS = 1000;
  public static final int REPLICATOR_APPEND_RPC_TIMEOUT_MILLISECONDS = 5000;
}
