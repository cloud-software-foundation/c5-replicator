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
package c5db.tablet;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

/**
 * Helper functionality to make various value type/objects.
 */
public class MetaTableNames {
  public static HTableDescriptor rootTableDescriptor() {
    return HTableDescriptor.ROOT_TABLEDESC;
  }

  public static TableName rootTableName() {
    return HTableDescriptor.ROOT_TABLE_NAME;
  }

  public static HRegionInfo rootRegionInfo() {
    return new HRegionInfo(
        rootTableName(),
        startKeyZero(),
        endKeyEmpty(),
        false,
        rootRegionId()
    );
  }

  private static int rootRegionId() {
    return 1;
  }

  private static byte[] startKeyZero() {
    return new byte[]{0};
  }

  private static byte[] endKeyEmpty() {
    return new byte[]{};
  }
}
