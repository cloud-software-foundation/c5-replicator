
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

/**
 * This package handles write-ahead logging for the C5 database. It provides and implements the
 * {@link c5db.log.OLog} abstraction, which allows logging for several different quorums (each
 * one representing the actions of a different region) potentially all interspersed within the same
 * log file on disk. It also provides an interface to that shared WAL for quorum-specific replicators
 * to use, so that conceptually each has its own WAL. And, it provides compatibility with
 * {@link org.apache.hadoop.hbase.regionserver.wal.HLog} in the form of {@link c5db.log.OLogShim}.
 */

package c5db.log;
