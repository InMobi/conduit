package com.inmobi.databus.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ParallelRecursiveListing {
  private static final Log LOG = LogFactory.getLog(ParallelRecursiveListing.class);
  // default value of number of threads to perform listing
  private int numThreads = 100;
  private ListingWorker [] listingWorkers = null;
  // list containing the pending paths to be recursively listed
  private LinkedList<Path> pendingPaths = new LinkedList<Path>();
  private int numListingsInProgress = 0;

  public ParallelRecursiveListing(int numThreads) {
    this.numThreads = numThreads;
  }
  
  public List<FileStatus> getListing(Path dir, FileSystem fs,
      boolean includeEmptyDir) {
    // add the starting path in the pending list
    pendingPaths.add(dir);
    
    // create a list of workers to perform recursive listing
    listingWorkers = new ListingWorker[numThreads];
    for (int i = 0; i < numThreads; i++) {
      listingWorkers[i] = new ListingWorker(fs, includeEmptyDir);
    }
    
    // start the listing workers
    for (int i = 0; i < numThreads; i++) {
      listingWorkers[i].start();
    }
    
    // wait for the listing workers to join
    for (int i = 0; i < numThreads; i++) {
      try {
        listingWorkers[i].join();
      } catch (InterruptedException e) {
        // ignore any spurious wakeup; continue in the loop
        continue;
      }
    }
    
    // add the file listings of each worker thread
    List<FileStatus> result = new ArrayList<FileStatus>();
    for (int i = 0; i < numThreads; i++) {
      result.addAll(listingWorkers[i].getFileStatus());
    }
    
    // sort the file listings
    Collections.sort(result, new DatePathComparator());
    return result;
  }
  
  public static void main(String[] args) {
    String pathStr = args[0];
    int numThreads = Integer.parseInt(args[1]);
    boolean includeEmptyDir = Integer.parseInt(args[2]) == 0 ? false : true;
    
    ParallelRecursiveListing listing = new ParallelRecursiveListing(numThreads);
    Path dir = new Path(pathStr);
    FileSystem fs = null;
    List<FileStatus> result = null;
    try {
      fs = dir.getFileSystem(new Configuration());
      long startTime = System.currentTimeMillis();
      result = listing.getListing(dir, fs, includeEmptyDir);
      long endTime = System.currentTimeMillis();
      System.out.println("The result contains entries: " + result.size() + 
          " time taken (ms): " + Long.toString(endTime - startTime));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  private class ListingWorker extends Thread {
    private FileSystem fs = null;
    private boolean includeEmptyDir = false;
    // file listing created by each worker thread
    private List<FileStatus> fileListing = new ArrayList<FileStatus>();
    // list of child dirs to be added back to pending list in any iteration
    private List<FileStatus> dirList = new ArrayList<FileStatus>();
    
    public ListingWorker(FileSystem fs, boolean includeEmptyDir) {
      this.fs = fs;
      this.includeEmptyDir = includeEmptyDir;
    }
    
    @Override
    public void run() {
      // check whether the list has any pending path
      while (true) {
        Path p = null;
        
        synchronized (pendingPaths) {
          // if pending list contains an entry, remove it and increment the counter
          p = pendingPaths.poll();
          if (p != null) {
            numListingsInProgress++;
          } else {
            if (numListingsInProgress == 0) {
              // if no pending path left and no thread is currently fetching
              return;
            } else {
              // some thread is currently fetching; wait for it to complete
              try {
                pendingPaths.wait();
              } catch (InterruptedException e) {
                // ignore any spurious wakeup; continue in the loop
                continue;
              }
            }
          }
        }
        
        if (p == null) {
          continue;
        }
        
        // perform listing for the path
        FileStatus[] fileStatuses = null;
        try {
          fileStatuses = fs.listStatus(p);
        } catch (IOException e) {
          LOG.debug("Error encountered while listing file status for path ["
              + p + "]. Reason: " + e.getMessage());
          decrementCounter();
          continue;
        }
        if (fileStatuses == null || fileStatuses.length == 0) {
          LOG.debug("No files in directory: " + p);
          // add the FileStatus of empty dir
          if (includeEmptyDir) {
            try {
              fileListing.add(fs.getFileStatus(p));
            } catch (IOException e) {
              LOG.debug("Error encountered while listing file status for path ["
                  + p + "]. Reason: " + e.getMessage());
              decrementCounter();
              continue;
            }
          }
        } else {
          for (FileStatus status : fileStatuses) {
            if (status.isDir()) {
              // if the child is a dir, add it to the dir list. All child dirs
              // will then be added together to the pending list.
              dirList.add(status);
            } else {
              // if it is a file, add it to the worker file listing set
              fileListing.add(status);
            }
          }
        }
        
        // add child dirs back to pending list and decrement counter
        decrementCounter();
      }
    }
    
    public List<FileStatus> getFileStatus() {
      return fileListing;
    }
    
    private void decrementCounter() {
      // decrement the counter and notify all waiting threads
      synchronized (pendingPaths) {
        // add any child dir paths back to the pending list
        if (dirList.size() > 0) {
          for (FileStatus status : dirList) {
            pendingPaths.add(status.getPath());
          }
          dirList.clear();
        }
        numListingsInProgress--;
        pendingPaths.notifyAll();
      }
    }
  }
}
