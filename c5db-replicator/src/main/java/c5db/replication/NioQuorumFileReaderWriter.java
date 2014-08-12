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

package c5db.replication;

import c5db.ReplicatorConstants;
import io.netty.util.CharsetUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class NioQuorumFileReaderWriter implements QuorumFileReaderWriter {
  private final Path basePath;

  public NioQuorumFileReaderWriter(Path basePath) {
    this.basePath = basePath.resolve(ReplicatorConstants.REPLICATOR_QUORUM_FILE_ROOT_DIRECTORY_RELATIVE_PATH);
  }

  @Override
  public List<String> readQuorumFile(String quorumId, String fileName) throws IOException {
    Path filePath = basePath.resolve(quorumId).resolve(fileName);

    try {
      return Files.readAllLines(filePath, CharsetUtil.UTF_8);
    } catch (NoSuchFileException ex) {
      return new ArrayList<>();
    }
  }

  @Override
  public void writeQuorumFile(String quorumId, String fileName, List<String> data) throws IOException {
    Path dirPath = basePath.resolve(quorumId);

    Files.createDirectories(dirPath);

    Path filePath = dirPath.resolve(fileName);
    Files.write(filePath, data, CharsetUtil.UTF_8);
  }
}
