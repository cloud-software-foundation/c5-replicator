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

public class ReplicatorConstants {
  public static final int REPLICATOR_PORT_MIN = 1024;

  public static final String REPLICATOR_QUORUM_FILE_ROOT_DIRECTORY_NAME = "repl";
  public static final String REPLICATOR_PERSISTER_FILE_NAME = "replication-data";
  public static final int REPLICATOR_NODE_INFO_REQUEST_TIMEOUT_MILLISECONDS = 3000;
  public static final int REPLICATOR_MAXIMUM_SIMULTANEOUS_LOG_REQUESTS = 10000;
  public static final int REPLICATOR_DEFAULT_BASE_ELECTION_TIMEOUT_MILLISECONDS = 1000;
  public static final int REPLICATOR_DEFAULT_ELECTION_CHECK_INTERVAL_MILLISECONDS = 100;
  public static final int REPLICATOR_DEFAULT_LEADER_LOG_INTERVAL_MILLISECONDS = 100;
  public static final int REPLICATOR_VOTE_RPC_TIMEOUT_MILLISECONDS = 1000;
  public static final int REPLICATOR_APPEND_RPC_TIMEOUT_MILLISECONDS = 5000;
}
