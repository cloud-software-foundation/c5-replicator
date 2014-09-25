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
