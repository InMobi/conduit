package com.inmobi.databus.utils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

public class TestParallelRecursiveListing {
  List<Path> expectedPaths = new ArrayList<Path>();
  Path startPath = null;
  Path endPath = null;
  
  private void cleanup(FileSystem fs, Path dir) throws Exception {
    fs.delete(dir, true);
    expectedPaths.clear();
  }
  
  @Test
  public void testListing() throws Exception {
    testListingWithEmptyDirs(true);
    testListingWithEmptyDirs(false);
  }
  
  private void testListingWithEmptyDirs(boolean includeEmptyDir) throws Exception {
    Path dir = new Path("file:///tmp/listingRootDir");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    // clean up the listing root dir before creating data
    cleanup(fs, dir);
    
    int numMinDirs = 100;
    int numMaxFiles = 10;
    int numThreads = 10;
    createData(dir, fs, numMinDirs, numMaxFiles, includeEmptyDir);
    
    long startTime = System.currentTimeMillis();
    ParallelRecursiveListing parallelListing =
        new ParallelRecursiveListing(numThreads, startPath, endPath);
    List<FileStatus> listedFileStatuses = parallelListing.getListing(
        dir, fs, includeEmptyDir);
    long endTime = System.currentTimeMillis();
    
    System.out.println("Expected paths: " + expectedPaths.size() + 
        " Found paths: " + listedFileStatuses.size() + " Listing time (ms): " +
        Long.toString(endTime - startTime));
    
    // compare the sizes of the expected and listed paths
    Assert.assertEquals(expectedPaths.size(), listedFileStatuses.size());
    List<Path> listedPaths = new ArrayList<Path>();
    prepareListOfPaths(listedFileStatuses, listedPaths);
    Assert.assertEquals(expectedPaths.size(), listedPaths.size());
    Assert.assertTrue(listedPaths.containsAll(expectedPaths));
    // finally cleanup the listingRootDir after listing data
    cleanup(fs, dir);
  }

  private void prepareListOfPaths(List<FileStatus> listOfFileStatuses,
      List<Path> listOfPaths) {
    for (FileStatus fileStatus : listOfFileStatuses) {
      listOfPaths.add(fileStatus.getPath());
    }
  }
  
  private void createData(Path dir, FileSystem fs, int numMinDirs, int numMaxFiles,
      boolean includeEmptyDir) throws Exception {
    Date date = new Date();
    // the number of minute directories to create
    for (int i = 0; i < numMinDirs; i++) {
      Path path = CalendarHelper.getPathFromDate(date, dir);
      fs.mkdirs(path);
      if (i == 0) {
        // set startPath to the 1st minute dir
        startPath = path;
      }
      // number of files to create within the minute dir
      int count = (int) (Math.random() * numMaxFiles);
      for (int j = 0; j < count; j++) {
        String fileNameStr = new String("file_" + getDateAsYYYYMMDDHHmm(date) + 
            "_" + j);
        Path file = new Path(path, fileNameStr + ".gz");
        try {
          fs.create(file);
        } catch (IOException e) {
          e.printStackTrace();
        }
        expectedPaths.add(file);
      }
      if (count == 0 && includeEmptyDir) {
        expectedPaths.add(path);
      }
      date = CalendarHelper.addAMinute(date);
    }
    // set endPath to the next minute after minDirs
    endPath = CalendarHelper.getPathFromDate(date, dir);
  }
  
  private static String getDateAsYYYYMMDDHHmm(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    return dateFormat.format(date);
  }
}
