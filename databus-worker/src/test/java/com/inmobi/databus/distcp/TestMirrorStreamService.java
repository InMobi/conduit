package com.inmobi.databus.distcp;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.databus.AbstractServiceTest;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.PublishMissingPathsTest;
import com.inmobi.databus.SourceStream;

public class TestMirrorStreamService extends MirrorStreamService
    implements AbstractServiceTest {
  private static final Log LOG = LogFactory
      .getLog(TestMirrorStreamService.class);
  
  private Cluster destinationCluster = null;
  private Cluster srcCluster = null;
  private FileSystem fs = null;
  private Map<String, List<String>> files = null;
  private Calendar behinddate = new GregorianCalendar();
  private long mergeCommitTime = 0;
  
  public TestMirrorStreamService(DatabusConfig config, Cluster srcCluster,
      Cluster destinationCluster, Cluster currentCluster,
      Set<String> streamsToProcess) throws Exception {
    super(config, srcCluster, destinationCluster, currentCluster,
        new FSCheckpointProvider(destinationCluster.getCheckpointDir()),
        streamsToProcess);
    this.destinationCluster = destinationCluster;
    this.srcCluster = srcCluster;
    this.fs = FileSystem.getLocal(new Configuration());
  }
  
  @Override
  protected void preExecute() throws Exception {
    try {
      mergeCommitTime = behinddate.getTimeInMillis();
      // PublishMissingPathsTest.testPublishMissingPaths(this, false);
      if (files != null)
        files.clear();
      files = null;
      files = new HashMap<String, List<String>>();
      behinddate.add(Calendar.HOUR_OF_DAY, -2);
      for (Map.Entry<String, SourceStream> sstream : getConfig()
          .getSourceStreams().entrySet()) {
        
        List<String> filesList = new ArrayList<String>();
        String listPath = srcCluster.getFinalDestDirRoot()
            + sstream.getValue().getName();     
        TestMergedStreamService.getAllFiles(new Path(listPath), fs, filesList);
        files.put(sstream.getValue().getName(), filesList);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertFalse(true);
      throw new RuntimeException("Error in MirrorStreamService Test PreExecute");
    } catch (AssertionError e) {
      e.printStackTrace();
      throw new RuntimeException("Error in MergedStreamService Test PreExecute");
    }
    
  }
  
  @Override
  protected void postExecute() throws Exception {
    try {
      for (Map.Entry<String, SourceStream> sstream : getConfig()
          .getSourceStreams().entrySet()) {
        // checking from next minute of behind time because the dummy commitpath 
        // can not be mirrored to dest cluster as it was created before merge 
        // stream service run.
        behinddate.add(Calendar.MINUTE, 1);
        PublishMissingPathsTest.VerifyMissingPublishPaths(fs, mergeCommitTime,
            behinddate, this.destinationCluster.getFinalDestDirRoot()
                + sstream.getValue().getName());
        
        List<String> filesList = files.get(sstream.getValue().getName());
        String commitpath = destinationCluster.getFinalDestDirRoot()
            + sstream.getValue().getName();
        List<String> commitPaths = new ArrayList<String>();
        TestMergedStreamService.getAllFiles(new Path(commitpath), fs, commitPaths);
        try {
          LOG.debug("Checking in Path for Mirror mapred Output, No. of files: "
              + commitPaths.size());
          
          for (int j = 0; j < filesList.size() - 1; ++j) {
            String checkpath = filesList.get(j);
            LOG.debug("Mirror Checking file: " + checkpath);
            Assert.assertTrue(commitPaths.contains(checkpath));
          }
        } catch (NumberFormatException e) {
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertFalse(true);
      throw new RuntimeException(
          "Error in MirrorStreamService Test PostExecute");
    } catch (AssertionError e) {
      e.printStackTrace();
      throw new RuntimeException(
          "Error in MergedStreamService Test PostExecute");
    }
  }
  
  public void runExecute() throws Exception {
    super.execute();
  }
  
  public void runPreExecute() throws Exception {
    preExecute();
  }
  
  public void runPostExecute() throws Exception {
    postExecute();
  }

  @Override
  public void publishMissingPaths(long commitTime) throws Exception {
    super.publishMissingPaths(fs, destinationCluster.getFinalDestDirRoot(), 
        commitTime);
  }
  
  @Override
  public Cluster getCluster() {
    return destinationCluster;
  }
  
  public FileSystem getFileSystem() {
    return fs;
  }
}
