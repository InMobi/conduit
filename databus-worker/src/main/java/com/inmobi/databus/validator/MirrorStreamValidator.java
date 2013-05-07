package com.inmobi.databus.validator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.distcp.MirrorStreamService;

public class MirrorStreamValidator {
  private static final Log LOG = LogFactory.getLog(MirrorStreamValidator.class);
  private DatabusConfig databusConfig = null;
  private String streamName = null;
  private boolean fix = false;
  TreeSet<FileStatus> toBeCopiedPaths = new TreeSet<FileStatus>();
  Cluster mergedCluster = null;
  Cluster mirrorCluster = null;

  public MirrorStreamValidator(DatabusConfig databusConfig, 
      String streamName, String clusterName, boolean fix) {
    this.databusConfig = databusConfig;
    this.streamName = streamName;
    this.fix = fix;  
    // get the source cluster where merge stream service is running
    mergedCluster = databusConfig.getPrimaryClusterForDestinationStream(streamName);
    // get the dest cluster where mirror stream service is running
    mirrorCluster = databusConfig.getClusters().get(clusterName);
  }

  public void execute() throws Exception {
    // perform recursive listing of paths in source cluster
    Path mergedPath = new Path(mergedCluster.getFinalDestDirRoot(), streamName);
    TreeSet<FileStatus> mergedFileListingSet = new TreeSet<FileStatus>();
    FileSystem mergedFs = FileSystem.get(mergedCluster.getHadoopConf());
    doRecursiveListing(mergedPath, mergedFileListingSet, mergedFs);
    
    // perform recursive listing of paths in target cluster
    Path mirrorPath = new Path(mirrorCluster.getFinalDestDirRoot(), streamName);
    TreeSet<FileStatus> mirrorFileListingSet = new TreeSet<FileStatus>();
    FileSystem mirrorFs = FileSystem.get(mirrorCluster.getHadoopConf());
    doRecursiveListing(mirrorPath, mirrorFileListingSet, mirrorFs);
    
    // compare the listings and find the missing paths
    List<Path> inconsistentData = new ArrayList<Path>();
    compareMergedAndMirror(mergedFileListingSet, mirrorFileListingSet,
        mergedPath.toString(), mirrorPath.toString(), inconsistentData, toBeCopiedPaths);
    
    // check if there are missing paths that need to be copied to mirror stream
    if (fix && toBeCopiedPaths.size() > 0) {
      LOG.debug("Number of missing paths to be copied: " + toBeCopiedPaths.size());
      
      for (FileStatus srcStatus : toBeCopiedPaths) {
        LOG.debug("Missing path to be copied: " + srcStatus.getPath());
      }
      
      // copy the missing paths
      copyMissingPaths();
    }
  }
  
  private void doRecursiveListing(Path dir, TreeSet<FileStatus> fileListingSet,
      FileSystem fs) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(dir);
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOG.debug("No files in directory: " + dir);
      // add the FileStatus of empty dir
      fileListingSet.add(fs.getFileStatus(dir));
    } else {
      for (FileStatus file : fileStatuses) {
        if (file.isDir()) {
          doRecursiveListing(file.getPath(), fileListingSet, fs);
        } else {
          // add the FileStatus of file
          fileListingSet.add(file);
        }
      } 
    }
  }
  
  /**
   * It compares the merged stream and mirror streams 
   * stores the missed paths and data replay paths in the inconsistent data List
   * @param mergedStreamFileSet : sorted set of files in the merged stream
   * @param mirrorStreamFileSet : sorted set of files in the mirror stream
   * @param mirrorStreamDirPath: mirror stream dir path for finding   
   *        minute dirs paths only
   * @param mergedStreamDirPath : merged stream dir path for finding 
   *        minute dirs paths only
   * @param inconsistentData : stores all the missed paths and data replay paths
   * @param toBeCopiedPaths : set of missing paths to be copied
   */
  void compareMergedAndMirror(TreeSet<FileStatus> mergedStreamFileSet, 
      TreeSet<FileStatus> mirrorStreamFileSet, String mirrorStreamDirPath, 
      String mergedStreamDirPath, List<Path> inconsistentData,
      TreeSet<FileStatus> toBeCopiedPaths) {
    int i;
    int j;
    int mergedStreamLen = mergedStreamDirPath.length();
    int mirrorStreamLen = mirrorStreamDirPath.length();
    
    FileStatus [] mergedStreamFiles = mergedStreamFileSet.toArray(new FileStatus[0]);
    FileStatus [] mirrorStreamFiles = mirrorStreamFileSet.toArray(new FileStatus[0]);
    
    for(i=0, j=0 ; i < mergedStreamFiles.length && 
        j < mirrorStreamFiles.length; i++, j++) {
      Path mergedStreamFilePath = mergedStreamFiles[i].getPath();
      Path mirrorStreamFilePath = mirrorStreamFiles[j].getPath();
      
      String mergedStreamFileRelPath = mergedStreamFilePath.toString().
          substring(mergedStreamLen);
      String mirrorStreamFileRelPath = mirrorStreamFilePath.toString().
          substring(mirrorStreamLen);
      
      if(!mergedStreamFileRelPath.equals(mirrorStreamFileRelPath)) {
        if(mergedStreamFileRelPath.compareTo(mirrorStreamFileRelPath) < 0) {
          if (j == 0) {
            System.out.println("purged path in the mirror stream " + 
                mergedStreamFilePath);
          } else {
            System.out.println("Missing file path : " + mergedStreamFilePath);        
            // Add the entry for missing file in toBeCopied map
            toBeCopiedPaths.add(mergedStreamFiles[i]);
          }
          inconsistentData.add(mergedStreamFilePath);
          --j;
        } else {
          if (i == 0) {
            System.out.println("purged path in the merged stream" + 
               mirrorStreamFilePath);
          } else {
            System.out.println("Data Replica : " + mirrorStreamFilePath);
          }
          inconsistentData.add(mirrorStreamFilePath);
          --i;
        }
      } else {
        // System.out.println("match between   " + i + " and  " + j);
      }    
    } 
    if((i == j) && i== mergedStreamFiles.length && j == mirrorStreamFiles.length) {
      System.out.println("There are no missing paths");
    } else {
      /* check whether there are any missing file paths or extra dummy files  
       * or not
       */
      if(i == mergedStreamFiles.length ) {
        for(;j < mirrorStreamFiles.length; j++) {
          Path mirrorStreamFilePath = mirrorStreamFiles[j].getPath();
          System.out.println("Extra files are in the Mirrored Stream: " + 
              mirrorStreamFilePath);  
          inconsistentData.add(mirrorStreamFilePath);
        }
      } else {
        for( ; i < mergedStreamFiles.length; i++) {
          Path mergedStreamFilePath = mergedStreamFiles[j].getPath();
          System.out.println("To be Mirrored files: " + mergedStreamFilePath);  
          inconsistentData.add(mergedStreamFilePath);
        }
      }
    } 
  }
  
  void copyMissingPaths() throws Exception {
    // create an instance of MirrorStreamFixService and invoke its execute()
    Set<String> streamsToProcess = new HashSet<String>();
    streamsToProcess.add(streamName);
    MirrorStreamFixService mirrorFixService = new MirrorStreamFixService(databusConfig,
        mergedCluster, mirrorCluster, streamsToProcess);
    
    // copy the missing paths through distcp and commit the copied paths
    mirrorFixService.execute();
  }
  
  class MirrorStreamFixService extends MirrorStreamService {
    public MirrorStreamFixService(DatabusConfig databusConfig, Cluster srcCluster,
        Cluster destCluster, Set<String> streamsToProcess) throws Exception {
      super(databusConfig, srcCluster, destCluster, null, null, streamsToProcess);
    }
    
    @Override
    protected Path getDistCpTargetPath() {
      // create a separate path for mirror stream fix service so that it doesn't
      // interfere with the path created for mirror stream distcp
      return new Path(getDestCluster().getTmpPath(), "distcp_mirror_fix_"
          + getSrcCluster().getName() + "_" + getDestCluster().getName())
          .makeQualified(getDestFs());
    }
    
    @Override
    protected Map<String, FileStatus> getDistCPInputFile() throws Exception {
      TreeMap<String, FileStatus> missingPathsMap = new TreeMap<String, FileStatus>();
      // get the final destination path for each source status. In case of mirror stream
      // service, it should preserve the complete source path.
      for (FileStatus srcStatus : toBeCopiedPaths) {
        missingPathsMap.put(getFinalDestinationPath(srcStatus), srcStatus);
      }
      return missingPathsMap;
    }
    
    public void execute() throws Exception {
      super.execute();
    }
    
    @Override
    protected void finalizeCheckPoints() {
      LOG.debug("Skipping update of checkpoints in mirror stream fix service run");
    }
  }
}
