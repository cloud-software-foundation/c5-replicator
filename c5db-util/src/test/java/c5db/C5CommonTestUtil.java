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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Common helper methods for testing C5
 */
public class C5CommonTestUtil {
  protected static final Logger LOG = LoggerFactory.getLogger(C5CommonTestUtil.class);

  public C5CommonTestUtil() {
  }

  /**
   * System property key to get base test directory value
   */
  public static final String BASE_TEST_DIRECTORY_KEY =
      "test.build.data.basedirectory";

  /**
   * Default base directory for test output.
   */
  public static final String DEFAULT_BASE_TEST_DIRECTORY = "target/test-data";

  /**
   * Directory where we put the data for this instance of C5CommonTestUtil
   */
  private File dataTestDir = null;

  /**
   * @return Where to write test data on local filesystem, specific to
   * the test. Useful for tests that do not use a cluster.
   * Creates it if it does not exist already.
   */
  public Path getDataTestDir() {
    if (this.dataTestDir == null) {
      setupDataTestDir();
    }
    return Paths.get(this.dataTestDir.getAbsolutePath());
  }

  /**
   * @param subdirName Test subdir name
   * @return Path to a subdirectory named <code>subdirName</code> under
   * {@link #getDataTestDir()}.
   * Does *NOT* create it if it does not exist.
   */
  public Path getDataTestDir(final String subdirName) {
    return Paths.get(getDataTestDir().toString(), subdirName);
  }

  /**
   * Sets up a directory for a test to use.
   *
   * @return New directory path, if created.
   */
  protected Path setupDataTestDir() {
    if (this.dataTestDir != null) {
      LOG.warn("Data test dir already setup in " +
          dataTestDir.getAbsolutePath());
      return null;
    }

    String randomStr = UUID.randomUUID().toString();
    Path testPath = Paths.get(getBaseTestDir().toString(), randomStr);

    this.dataTestDir = new File(testPath.toString()).getAbsoluteFile();
    // Set this property so if mapreduce jobs run, they will use this as their home dir.
    System.setProperty("test.build.dir", this.dataTestDir.toString());
    if (deleteOnExit()) {
      this.dataTestDir.deleteOnExit();
    }

    createSubDir("c5.local.dir", testPath, "c5-local-dir");

    return testPath;
  }

  protected void createSubDir(String propertyName, Path parent, String subDirName) {
    Path newPath = Paths.get(parent.toString(), subDirName);
    File newDir = new File(newPath.toString()).getAbsoluteFile();
    if (deleteOnExit()) {
      newDir.deleteOnExit();
    }
  }

  /**
   * @return True if we should delete testing dirs on exit.
   */
  boolean deleteOnExit() {
    String v = System.getProperty("c5.testing.preserve.testdir");
    // Let default be true, to delete on exit.
    return v == null || !Boolean.parseBoolean(v);
  }

  /**
   * @return True if we removed the test dirs
   * @throws java.io.IOException
   */
  public boolean cleanupTestDir() throws IOException {
    if (deleteDir(this.dataTestDir)) {
      this.dataTestDir = null;
      return true;
    }
    return false;
  }

  /**
   * @param subdir Test subdir name.
   * @return True if we removed the test dir
   * @throws java.io.IOException
   */
  boolean cleanupTestDir(final String subdir) throws IOException {
    if (this.dataTestDir == null) {
      return false;
    }
    return deleteDir(new File(this.dataTestDir, subdir));
  }

  /**
   * @return Where to write test data on local filesystem; usually
   * {@link #DEFAULT_BASE_TEST_DIRECTORY}
   * Should not be used by the unit tests, hence it's private.
   * Unit test will use a subdirectory of this directory.
   * @see #setupDataTestDir()
   */
  private Path getBaseTestDir() {
    String PathName = System.getProperty(
        BASE_TEST_DIRECTORY_KEY, DEFAULT_BASE_TEST_DIRECTORY);

    return Paths.get(PathName);
  }

  /**
   * @param dir Directory to delete
   * @return True if we deleted it.
   * @throws java.io.IOException
   */
  boolean deleteDir(final File dir) throws IOException {
    if (dir == null || !dir.exists()) {
      return true;
    }
    try {
      if (deleteOnExit()) {
        Files.delete(dir.toPath());
      }
      return true;
    } catch (IOException ex) {
      LOG.warn("Failed to delete " + dir.getAbsolutePath());
      return false;
    }
  }
}
