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

package org.apache.hadoop.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Credentials;

/**
 * FileBasedCopyListing implements the CopyListing interface,
 * to create the copy-listing for DistCp,
 * by iterating over all source paths mentioned in a specified input-file.
 */
public class FileBasedCopyListing extends CopyListing {

  private final CopyListing globbedListing;
  /**
   * Constructor, to initialize base-class.
   * @param configuration: The input Configuration object.
   * @param credentials - Credentials object on which the FS delegation tokens are cached. If null
   * delegation token caching is skipped
   */
  public FileBasedCopyListing(Configuration configuration, Credentials credentials) {
    super(configuration, credentials);
    globbedListing = new GlobbedCopyListing(getConf(), credentials);
  }

  /** {@inheritDoc} */
  @Override
  protected void validatePaths(DistCpOptions options)
      throws IOException, InvalidInputException {
  }

  /**
   * Implementation of CopyListing::buildListing().
   *   Iterates over all source paths mentioned in the input-file.
   * @param pathToListFile: Path on HDFS where the listing file is written.
   * @param options: Input Options for DistCp (indicating source/target paths.)
   * @throws IOException
   */
  @Override
  public void doBuildListing(Path pathToListFile, DistCpOptions options)
      throws IOException {
    List<Path> sourcePaths = fetchFileList(options.getSourceFileListing(),
        getConf());
    DistCpOptions newOption = new DistCpOptions(sourcePaths,
        options.getTargetPath());
    newOption.setSyncFolder(options.shouldSyncFolder());
    newOption.setOverwrite(options.shouldOverwrite());
    newOption.setDeleteMissing(options.shouldDeleteMissing());
    newOption.setPreserveSrcPath(options.shouldPreserveSrcPath());
    newOption.setSkipPathValidation(options.isSkipPathValidation());
    newOption.setUseSimpleFileListing(options.isUseSimpleFileListing());

    globbedListing.buildListing(pathToListFile, newOption);
  }

  protected static List<Path> fetchFileList(Path sourceListing,
      Configuration conf) throws IOException {
    List<Path> result = new ArrayList<Path>();
    FileSystem fs = sourceListing.getFileSystem(conf);
    BufferedReader input = null;
    try {
      input = new BufferedReader(new InputStreamReader(fs.open(sourceListing)));
      String line = input.readLine();
      while (line != null) {
        result.add(new Path(line));
        line = input.readLine();
      }
    } finally {
      IOUtils.closeStream(input);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  protected long getBytesToCopy() {
    return globbedListing.getBytesToCopy();
  }

  /** {@inheritDoc} */
  @Override
  protected long getNumberOfPaths() {
    return globbedListing.getNumberOfPaths();
  }
}
