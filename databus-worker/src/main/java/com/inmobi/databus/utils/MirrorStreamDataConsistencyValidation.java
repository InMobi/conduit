package com.inmobi.databus.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import com.inmobi.databus.distcp.DatabusDistCp;

/**
 * This class checks the data consistency between merged stream and mirrored 
 * stream for different clusters. It returns the both missed paths and data 
 * replay paths.
 * This class main method takes merged stream, comma separated list of mirror 
 * stream urls and comma separated list of stream names
 *
 */
public class MirrorStreamDataConsistencyValidation {
  private static final Log LOG = LogFactory.getLog(
      MirrorStreamDataConsistencyValidation.class);
  List<Path> mirrorStreamDirs;
  Path mergedStreamDirPath;

  public MirrorStreamDataConsistencyValidation(String mirrorStreamUrls, 
      String mergedStreamUrl) throws IOException {
    String[] rootDirSplits;
    mergedStreamDirPath = new Path(mergedStreamUrl, "streams");
    if (mirrorStreamUrls != null) {
      rootDirSplits = mirrorStreamUrls.split(",");
    } else {
      throw new IllegalArgumentException("Databus root directory not specified");
    }
    mirrorStreamDirs = new ArrayList<Path>(rootDirSplits.length);
    for (String mirrorRootDir : rootDirSplits) {
      mirrorStreamDirs.add(new Path(mirrorRootDir, "streams"));
    }
  }

  public List<Path> processListingStreams(String streamName, 
      boolean copyMissingPaths) throws Exception {
    List<Path> inconsistentData = new ArrayList<Path>();
    TreeSet<FileStatus> filesInMergedStream = new TreeSet<FileStatus>();
    mergedStreamListing(streamName, filesInMergedStream);
    mirrorStreamListing(streamName, inconsistentData, filesInMergedStream, 
        copyMissingPaths);
    return inconsistentData;
  }

  public void mergedStreamListing(String streamName, TreeSet<FileStatus>  
  filesInMergedStream) throws IOException {
    Path completeMergedStreamDirPath = new Path(mergedStreamDirPath,
        streamName);
    LOG.info("merged Stream path : " + completeMergedStreamDirPath);    
    FileSystem mergedFS = completeMergedStreamDirPath.getFileSystem(
        new Configuration());
    doRecursiveListing(completeMergedStreamDirPath, filesInMergedStream,
        mergedFS);             

    for (FileStatus status : filesInMergedStream) {
      LOG.debug(" files in merged stream: " + status.getPath());
    }
  }

  public void mirrorStreamListing(String streamName, List<Path> inconsistentData,
      TreeSet<FileStatus> filesInMergedStream, boolean copyMissingPaths)
      throws Exception {
    Path mirrorStreamDirPath;
    FileSystem mirroredFs;
    for (int i = 0; i < mirrorStreamDirs.size(); i++) {
      TreeSet<FileStatus> filesInMirroredStream = new TreeSet<FileStatus>();
      mirrorStreamDirPath = new Path(mirrorStreamDirs.get(i), streamName);
      mirroredFs = mirrorStreamDirPath.getFileSystem(new Configuration());
      LOG.info("mirroredStream Path : " + mirrorStreamDirPath);
      
      doRecursiveListing(mirrorStreamDirPath, filesInMirroredStream, mirroredFs);
      
      for (FileStatus status : filesInMirroredStream) {
        LOG.debug(" files in mirrored stream: " + status.getPath());
      }
      System.out.println("stream name: " + streamName);
      
      TreeMap<String, FileStatus> toBeCopiedPaths = new TreeMap<String, FileStatus>();
      compareMergedAndMirror(filesInMergedStream, filesInMirroredStream, 
          mirrorStreamDirs.get(i).toString(), mergedStreamDirPath.
          toString(), inconsistentData, toBeCopiedPaths);
      
      // check if there are missing paths that need to be copied to mirror stream
      if (toBeCopiedPaths.size() > 0 && copyMissingPaths) {
        for (String dest : toBeCopiedPaths.keySet()) {
          LOG.debug("Missing path to be copied: " + dest);
        }
        
        // perform distcp copy of missing paths
        Path mirrorRootDir = mirrorStreamDirs.get(i).getParent();
        copyMissingPaths(toBeCopiedPaths, mirroredFs, mirrorRootDir, streamName);
      }
    }
  }

  /**
   * It compares the merged stream and mirror streams 
   * stores the missed paths and data replay paths in the inconsistent data List
   * @param mergedStreamFileSet : sorted set of files in the merged stream
   * @param mirrorStreamFileSet : sorted set of files in the mirrored stream
   * @param mirrorStreamDirPath: mirror stream dir path for finding   
   * 				minute dirs paths only
   * @param mergedStreamDirPath : merged stream dir path for finding 
   * 			  minute dirs paths only
   * @param inconsistentData : stores all the missed paths and data replay paths
   * @param toBeCopiedPaths : map of missing entries to be copied
   */
  void compareMergedAndMirror(TreeSet<FileStatus> mergedStreamFileSet, 
      TreeSet<FileStatus> mirrorStreamFileSet, String mirrorStreamDirPath, 
      String mergedStreamDirPath, List<Path> inconsistentData,
      TreeMap<String, FileStatus> toBeCopiedPaths) {
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
            toBeCopiedPaths.put(mergedStreamFileRelPath, mergedStreamFiles[i]);
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
  
  void copyMissingPaths(TreeMap<String, FileStatus> toBeCopiedPaths, FileSystem 
      mirroredFs, Path mirrorRootDir, String streamName) throws Exception {
    System.out.println("Number of paths to be copied: " + toBeCopiedPaths.size());
    
    Path tmpPath = new Path(mirrorRootDir + File.separator + "system" + 
        File.separator + "tmp");
    Path tmpOut = new Path(tmpPath, "distcp_mirror_consistency_" + streamName).
        makeQualified(mirroredFs);
    
    // Cleanup tmpOut before every run
    if (mirroredFs.exists(tmpOut))
      mirroredFs.delete(tmpOut, true);
    
    if (!mirroredFs.mkdirs(tmpOut)) {
      LOG.warn("Cannot create [" + tmpOut + "]..skipping this run");
      return;
    }
        
    DistCpOptions options = new DistCpOptions(new Path("/tmp"), tmpOut);
    DistCp distCp = new DatabusDistCp(new Configuration(), options, toBeCopiedPaths);
    
    try {
      distCp.execute();
    } catch (Exception e) {
      LOG.error("Exception encountered: ", e);
      throw e;
    }
  }

  /**
   * It lists all the dirs and file paths which are presented in 
   * the stream directory
   * @param dir : stream directory path 
   * @param fileListingSet : sorted set of source FileStatus
   * @param fs : FileSystem object
   */
  public void doRecursiveListing(Path dir, TreeSet<FileStatus> fileListingSet,
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

  public List<Path> run(String [] args) throws Exception {
    List<String> streamNames = new ArrayList<String>();
    List<Path> inconsistentData = new ArrayList<Path>();
    boolean copyMissingPaths = false;
    
    if (args.length == 2) {
      // get all stream names
      FileSystem fs = mirrorStreamDirs.get(0).getFileSystem(new Configuration());
      FileStatus[] fileStatuses = fs.listStatus(mirrorStreamDirs.get(0));
      if (fileStatuses != null && fileStatuses.length != 0) {
        for (FileStatus file : fileStatuses) {  
          streamNames.add(file.getPath().getName());
        } 
      } else {
        System.out.println("There are no stream names in the mirrored stream");
      }
    } else if (args.length == 3) {
      if (args[2].equalsIgnoreCase("-fix")) {
        copyMissingPaths = true;
        // get all stream names
        FileSystem fs = mirrorStreamDirs.get(0).getFileSystem(new Configuration());
        FileStatus[] fileStatuses = fs.listStatus(mirrorStreamDirs.get(0));
        if (fileStatuses != null && fileStatuses.length != 0) {
          for (FileStatus file : fileStatuses) {  
            streamNames.add(file.getPath().getName());
          } 
        } else {
          System.out.println("There are no stream names in the mirrored stream");
        }
      } else {
        // the 3rd argument is a list of stream names
        for (String streamname : args[2].split(",")) {
          streamNames.add(streamname);
        }
      }
    } else if (args.length == 4) {
      // the 3rd argument is a list of stream names
      for (String streamname : args[2].split(",")) {
        streamNames.add(streamname);
      }
      // the 4th argument is whether missing paths need to be copied
      if (args[3].equalsIgnoreCase("-fix")) {
        copyMissingPaths = true;
      }
    }
    
    for (String streamName : streamNames) {
      inconsistentData.addAll(this.processListingStreams(streamName, copyMissingPaths));
    }
    if (inconsistentData.isEmpty()) {
      System.out.println("there is no inconsistency data");
    }
    return inconsistentData;
  }

  static void printUsage() {
    System.out.println("Usage: mirrorstreamdataconsistency  "
        + " <mergedstream root-dir>"
        + " <mirrorstream root-dir (comma separated list)>"
        + " [<streamname (comma separated list)>]"
        + " [-fix (needs shutdown of merge and mirror databus workers) ");
  }
  
  public static void main(String args[]) throws Exception {
    if (args.length >= 2) {
      String mergedStreamUrl = args[0];
      String mirrorStreamUrls = args[1];	
      MirrorStreamDataConsistencyValidation obj = new 
          MirrorStreamDataConsistencyValidation(mirrorStreamUrls, 
              mergedStreamUrl);
      obj.run(args);
    } else {
      printUsage();
      System.exit(1);
    }
  }
}
