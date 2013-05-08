package com.inmobi.databus.validator;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public abstract class AbstractStreamValidator {
  private static final Log LOG = LogFactory.getLog(AbstractStreamValidator.class);
  Map<String, FileStatus> missingPaths = new TreeMap<String, FileStatus>();

  protected void findDuplicates(List<FileStatus> listOfFileStatuses,
      Map<String, FileStatus> streamListingMap) {
    String fileName;
    for (FileStatus fileStatus : listOfFileStatuses) {
      fileName = getFinalDestinationPath(fileStatus);
      if (streamListingMap.containsKey(fileName)) {
        Path duplicatePath;
        Path existingPath = streamListingMap.get(fileName).getPath();
        Path currentPath = fileStatus.getPath();
        if (existingPath.compareTo(currentPath) < 0) {
          duplicatePath = currentPath;
        } else {
          duplicatePath = existingPath;
          // insert this entry into the map because this file was created first
          streamListingMap.put(fileName, fileStatus);
        }
        LOG.debug("Duplicate file " + duplicatePath);
      } else {
        streamListingMap.put(fileName, fileStatus);
      }
    }
  }
  
  protected void findMissingPaths(Map<String, FileStatus> srcListingMap,
      Map<String, FileStatus> destListingMap) {
    String fileName = null;
    for (Map.Entry<String, FileStatus> srcEntry : srcListingMap.entrySet()) {
      fileName = srcEntry.getKey();
      if (!destListingMap.containsKey(fileName)) {
        LOG.debug("Missing path " + srcEntry.getValue().getPath());
        missingPaths.put(fileName, srcEntry.getValue());
      }
    }
  }
  
  protected List<FileStatus> findHoles(List<FileStatus> listOfFileStatuses) {
    return null;
  }
  
  protected abstract String getFinalDestinationPath(FileStatus srcPath);
}
