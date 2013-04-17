/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.inmobi.databus.distcp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import com.inmobi.databus.utils.FileUtil;

/**
 * This class extends DistCp class and overrides createInputFileListing()
 * method that writes the listing file directly using the file listing map
 * passed by merge/mirror stream service.
 */
public class DatabusDistCp extends DistCp {
  
  private static final Log LOG = LogFactory.getLog(DistCp.class);
  
  // The fileList map contains key as the destination name, 
  // and source as FileStatus
  private Map<String, FileStatus> fileListingMap = null;
  // counter for the total number of paths to be copied
  private long totalPaths = 0;
  //counter for the total bytes to be copied
  private long totalBytesToCopy = 0;
  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(64);
  private DataInputBuffer in = new DataInputBuffer();

  public DatabusDistCp(Configuration configuration, DistCpOptions inputOptions,
      Map<String, FileStatus> fileListingMap)
      throws Exception {
    super(configuration, inputOptions);
    this.fileListingMap = fileListingMap;
  }
  
  //TODO: handle any distp counters related logic that need to be added here
  protected Path createInputFileListing(Job job) throws IOException {
    // get the file path where copy listing file has to be saved
    Path fileListingPath = getFileListingPath();
    
    SequenceFile.Writer fileListWriter = null;
    try {
      fileListWriter = SequenceFile.createWriter(fileListingPath.getFileSystem(getConf()),
          getConf(), fileListingPath, Text.class, FileStatus.class, 
          SequenceFile.CompressionType.NONE);
      
      for (Map.Entry<String, FileStatus> entry : fileListingMap.entrySet()) {
        FileStatus status = FileUtil.getFileStatus(entry.getValue(), buffer, in);
        fileListWriter.append(new Text(entry.getKey()), status);
        
        // Create a sync point after each entry. This will ensure that SequenceFile
        // Reader can work at file entry level granularity, given that SequenceFile
        // Reader reads from the starting of sync point.
        fileListWriter.sync();
        
        totalBytesToCopy += entry.getValue().getLen();
        totalPaths++;
      }
    } finally {
      if (fileListWriter != null)  {
        fileListWriter.close();
      }
    }
    
    LOG.info("Number of paths considered for copy: " + totalPaths);
    LOG.info("Number of bytes considered for copy: " + totalBytesToCopy
            + " (Actual number of bytes copied depends on whether any files are "
            + "skipped or overwritten.)");
    
    return fileListingPath;
  }

}
