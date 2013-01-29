package com.inmobi.databus.utils;

import java.io.IOException;
import java.util.List;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;

public abstract class CompareDataConsistency {
  
  /**
   * this method identifies the purged paths on the source streams and destination
   *  streams
   */
  public void skipPurgedPaths(TreeMap<String, Path> sourceStreamFiles, 
      TreeMap<String, Path> destStreamFiles, List<Path> inconsistency) {

    Set<Entry<String, Path>> sourceStreamFileEntries = 
        getStreamEntries(sourceStreamFiles);
    Set<Entry<String, Path>> destStreamFileEntries = 
        getStreamEntries(destStreamFiles);
    Iterator<Entry<String, Path>> sourcestreamIt = sourceStreamFileEntries
        .iterator();
    Iterator<Entry<String, Path>> destStreamIt = destStreamFileEntries
        .iterator();
    String sourceStreamKey = null;
    String destStreamKey = null;
    if (sourcestreamIt.hasNext()) {
      sourceStreamKey = getNextKey(sourcestreamIt);
    }
    if (destStreamIt.hasNext()) {
      destStreamKey = getNextKey(destStreamIt);
    }
    
    int missingfilesCount = checkPurgePaths(sourceStreamKey, destStreamKey, 
        sourcestreamIt, inconsistency, sourceStreamFiles); 
    // missingfilesCount variable is used to know purging was happened on the 
    //source or dest stream 
    if (missingfilesCount == 0) {
      checkPurgePaths( destStreamKey, sourceStreamKey,destStreamIt,
          inconsistency, destStreamFiles);
    }  
  }

  protected String getNextKey(Iterator<Entry<String, Path>> streamIt) {
    return streamIt.next().getKey();
  }
  
  public int checkPurgePaths(String sourceStreamKey, String destStreamKey, 
      Iterator<Entry<String, Path>> streamIt, List<Path> inconsistency, 
      TreeMap<String, Path> streamFiles) {
    boolean breakflag = false;
    int loopcount = 0;
    while ((sourceStreamKey != null) && (destStreamKey != null)) {
      if (!sourceStreamKey.equals(destStreamKey)) {
        if (sourceStreamKey.compareTo(destStreamKey) < 0) {
          inconsistency.add(streamFiles.get(sourceStreamKey));
          System.out.println("purged path: " + sourceStreamKey);
          streamIt.remove();
          loopcount++;
          if (streamIt.hasNext()) {
            sourceStreamKey = getNextKey(streamIt);  
          } else {
            breakflag = true;
          }
        } else {
          breakflag = true;
        }
      } else {
        breakflag = true;
      }
      if (breakflag) {
        break;
      }
    }
    return loopcount;
  }

  protected Set<Entry<String, Path>> getStreamEntries(
      TreeMap<String, Path> streamFiles) {
    return streamFiles.entrySet();
  }

  /**
   * This method compares the data consistency between source and destination streams
   * @return list of inconsistency paths
   */
  public List<Path> compareDataConsistency(TreeMap<String, Path> sourceStreamFiles, 
      TreeMap<String, Path> destStreamFiles, List<Path> inconsistency) {
    
    skipPurgedPaths(sourceStreamFiles, destStreamFiles, inconsistency);
    
    Set<Entry<String, Path>> sourceStreamFileEntries = 
        getStreamEntries(sourceStreamFiles);
    Set<Entry<String, Path>> destStreamFileEntries = 
        getStreamEntries(destStreamFiles);
  
    Iterator<Entry<String, Path>> sourcestreamIt = sourceStreamFileEntries.iterator();
    Iterator<Entry<String, Path>> destStreamIt = destStreamFileEntries.iterator();
    String sourceStreamKey = null;
    String destStreamKey = null;
    
    if (sourcestreamIt.hasNext()) {
      sourceStreamKey = getNextKey(sourcestreamIt);
    }
    if (destStreamIt.hasNext()) {
      destStreamKey = getNextKey(destStreamIt);
    }
    
    while ((sourceStreamKey != null) && (destStreamKey != null)) {
      if (!sourceStreamKey.equals(destStreamKey)) {
        if(sourceStreamKey.compareTo(destStreamKey) < 0) {
          System.out.println("missing path: " + sourceStreamFiles.get(sourceStreamKey));
          inconsistency.add(sourceStreamFiles.get(sourceStreamKey));
          if (sourcestreamIt.hasNext()) {
            sourceStreamKey = getNextKey(sourcestreamIt);
          } else {
            sourceStreamKey = null;
          }
        } else {
          System.out.println("data replay: " + destStreamFiles.get(destStreamKey));
          inconsistency.add(destStreamFiles.get(destStreamKey));
          if (destStreamIt.hasNext()) { 
            destStreamKey = getNextKey(destStreamIt);
          } else {
            destStreamKey = null;
          }
        }
      } else {
        if (sourcestreamIt.hasNext() && !destStreamIt.hasNext()) {
          sourceStreamKey = getNextKey(sourcestreamIt);
          destStreamKey = null;
        } else if (destStreamIt.hasNext() && !sourcestreamIt.hasNext()) {
          destStreamKey = getNextKey(destStreamIt);
          sourceStreamKey = null;
        } else if (sourcestreamIt.hasNext() && destStreamIt.hasNext()) {
          sourceStreamKey = getNextKey(sourcestreamIt);
          destStreamKey = getNextKey(destStreamIt);
        } else {
          sourceStreamKey = null;
          destStreamKey = null;
        }
      }
    }
    if ((sourceStreamFiles.size() == destStreamFiles.size()) &&
        sourceStreamKey == null && destStreamKey == null) {
      System.out.println("there are no missing files");
    } else {
      if (destStreamKey == null) {
        while (sourceStreamKey != null) {
          inconsistency.add(sourceStreamFiles.get(sourceStreamKey));
          System.out.println("Files to be sent: " +
              sourceStreamFiles.get(sourceStreamKey));
          if (sourcestreamIt.hasNext()) {
            sourceStreamKey = getNextKey(sourcestreamIt);
          } else {
            sourceStreamKey = null;
          }
        }
      } else {
        while (destStreamKey != null) {
          inconsistency.add(destStreamFiles.get(destStreamKey));
          System.out.println("extra files in stream: " +
              destStreamFiles.get(destStreamKey));
          if (destStreamIt.hasNext()) {
            destStreamKey = getNextKey(destStreamIt);
          } else {
            destStreamKey = null;
          }
        }
      }
    }
    return inconsistency;
  }
}
