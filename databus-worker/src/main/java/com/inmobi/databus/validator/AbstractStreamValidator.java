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
  
  protected List<FileStatus> findHoles(List<FileStatus> listOfFileStatuses) {
    return null;
  }
  
  protected abstract String getFinalDestinationPath(FileStatus srcPath);
}
