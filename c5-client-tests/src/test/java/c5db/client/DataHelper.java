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
package c5db.client;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

/**
 * language to help verify data from the database.
 *
 * Add additional static verbs as necessary!
 */
public class DataHelper {
  public static byte[] valueReadFromDatabase(byte[] row, C5Table c5Table) throws IOException {
    Get get = new Get(row);
    get.addColumn(row, row);
    Result result = c5Table.get(get);
    return result.getValue(row, row);
  }
}
