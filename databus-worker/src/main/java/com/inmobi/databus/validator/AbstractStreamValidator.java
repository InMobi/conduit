package com.inmobi.databus.validator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.utils.CalendarHelper;

public abstract class AbstractStreamValidator {
  private static final Log LOG = LogFactory.getLog(AbstractStreamValidator.class);
  Map<String, FileStatus> missingPaths = new TreeMap<String, FileStatus>();

  /**
   * @return the missingPaths
   */
  public Map<String, FileStatus> getMissingPaths() {
    return missingPaths;
  }

  protected void fixHoles(List<Path> holesList, FileSystem fs)
      throws IOException {
    for (Path holePath : holesList) {
      if (!fs.exists(holePath)) {
        fs.mkdirs(holePath);
      }
    }
    holesList.clear();
  }

  protected List<Path> findHoles(List<FileStatus> listOfFileStatuses,
      Path streamDir, FileSystem fs) throws IOException {
    List<Path> holes = new ArrayList<Path>();
    List<Path> listOfDirs = new ArrayList<Path>();
    // prepare a list of dirs from list of all files
    prepareListWithOnlyMinuteDirs(listOfFileStatuses, listOfDirs, streamDir);
    Collections.sort(listOfDirs);
    if (listOfDirs.isEmpty()) {
      return holes;
    }
    Path previousFile = null;
    Calendar cal = Calendar.getInstance();
    Path missingPath = null;
    for (Path currentFile : listOfDirs) {
      // read first file and set time stamp of file.
      if (previousFile == null) {
        previousFile = currentFile;
        Date previousFileTimeStamp = CalendarHelper.getDateFromStreamDir(
            streamDir, previousFile);
        cal.setTime(previousFileTimeStamp);
        continue;
      }
      cal.add(Calendar.MINUTE, 1);
      Date currentFileTimeStamp = CalendarHelper.getDateFromStreamDir(streamDir,
          currentFile);
      while (currentFileTimeStamp.compareTo(cal.getTime()) != 0) {
        missingPath = new Path(streamDir,
            Cluster.getDateAsYYYYMMDDHHMNPath(cal.getTime()));
        holes.add(missingPath);
        cal.add(Calendar.MINUTE, 1);
      }
    }
    return holes;
  }

  private void prepareListWithOnlyMinuteDirs(List<FileStatus> listOfFileStatuses,
      List<Path> listOfDirs, Path streamDir) {
    for (FileStatus fileStatus : listOfFileStatuses) {
      Path filePath = fileStatus.getPath();
      if (fileStatus.isDir()) {
        if (isMinuteDir(filePath, streamDir)) {
          listOfDirs.add(filePath);
        }
      } else {
        Path parentPath = filePath.getParent();
        if (!listOfDirs.contains(parentPath)) {
          listOfDirs.add(parentPath);
        }
      }
    }
  }

  private boolean isMinuteDir(Path filePath, Path streamDir) {
    Path streamDirFromPath = filePath.getParent().getParent().getParent().
        getParent().getParent();
    if (streamDirFromPath.equals(streamDir)) {
      return true;
    }
    return false;
  }

  protected abstract String getFinalDestinationPath(FileStatus srcPath);
}
