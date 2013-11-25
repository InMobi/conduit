package com.inmobi.databus.distcp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TDeserializer;
import org.testng.Assert;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.databus.AbstractServiceTest;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.Databus;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.PublishMissingPathsTest;
import com.inmobi.databus.SourceStream;
import com.inmobi.databus.utils.DatePathComparator;
import com.inmobi.databus.utils.FileUtil;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.publisher.MockInMemoryPublisher;
import com.inmobi.messaging.util.AuditUtil;

public class TestMergedStreamService extends MergedStreamService implements
    AbstractServiceTest {
  private static final Log LOG = LogFactory
      .getLog(TestMergedStreamService.class);

  private Cluster destinationCluster = null;
  private Cluster srcCluster = null;
  private FileSystem fs = null;
  private Map<String, List<String>> files = null;
  private Calendar behinddate = new GregorianCalendar();
  private Date todaysdate = null;

  public TestMergedStreamService(DatabusConfig config, Cluster srcCluster,
      Cluster destinationCluster, Cluster currentCluster,
      Set<String> streamsToProcess)
      throws Exception {
    super(config, srcCluster, destinationCluster, currentCluster,
        new FSCheckpointProvider(destinationCluster.getCheckpointDir()),
        streamsToProcess);
    MessagePublisher publisher = MessagePublisherFactory.create();
    Databus.setPublisher(publisher);
    this.srcCluster = srcCluster;
    this.destinationCluster = destinationCluster;
    this.fs = FileSystem.getLocal(new Configuration());
  }

  /*
   * Returns the last file path
   */
  public static FileStatus getAllFiles(Path listPath, FileSystem fs,
      List<String> fileList) throws IOException {

    FileStatus lastFile = null;
    DatePathComparator comparator = new DatePathComparator();
    FileStatus[] fileStatuses = null;
    try {
      fileStatuses = fs.listStatus(listPath);
    } catch (FileNotFoundException e) {
    }
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOG.debug("No files in directory:" + listPath);
      if (fs.exists(listPath))
        lastFile = fs.getFileStatus(listPath);
    } else {

      for (FileStatus file : fileStatuses) {
        if (file.isDir()) {
          lastFile = getAllFiles(file.getPath(), fs, fileList);
        } else {
          if (lastFile == null)
            lastFile = fileStatuses[0];
          if (comparator.compare(file, lastFile) > 0)
            lastFile = file;
          fileList.add(file.getPath().getName());
        }
      }
    }
    return lastFile;
  }

  @Override
  protected void preExecute() throws Exception {
    try {
      if (files != null)
        files.clear();
      files = null;
      files = new HashMap<String, List<String>>();
      behinddate.add(Calendar.HOUR_OF_DAY, -2);
      for (Map.Entry<String, SourceStream> sstream : getConfig()
          .getSourceStreams().entrySet()) {

        LOG.debug("Working for Stream in Merged Stream Service "
            + sstream.getValue().getName());

        List<String> filesList = new ArrayList<String>();
        String listPath = srcCluster.getLocalFinalDestDirRoot()
            + sstream.getValue().getName();

        LOG.debug("Getting List of Files from Path: " + listPath);
        getAllFiles(new Path(listPath), fs, filesList);
        files.put(sstream.getValue().getName(), filesList);
        LOG.debug("Creating Dummy commit Path for verifying Missing Paths");
        String dummycommitpath = this.destinationCluster.getFinalDestDirRoot()
            + sstream.getValue().getName() + File.separator
            + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
        fs.mkdirs(new Path(dummycommitpath));
      }
      // Copy AuditUtil src jar to FS
      String auditSrcJar = FileUtil.findContainingJar(
          com.inmobi.messaging.util.AuditUtil.class);
      fs.copyFromLocalFile(new Path(auditSrcJar), auditUtilJarDestPath);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Error in MergedStreamService Test PreExecute");
    } catch (AssertionError e) {
      e.printStackTrace();
      throw new RuntimeException("Error in MergedStreamService Test PreExecute");
    }
    todaysdate = null;
    todaysdate = new Date();

  }

  @Override
  protected void postExecute() throws Exception {
    try {
      int totalFileProcessedInRun = 0;
      for (String sstream : getConfig().getClusters().
          get(destinationCluster.getName()).getPrimaryDestinationStreams()) {
        if (srcCluster.getSourceStreams().contains(sstream)) {
          List<String> filesList = files.get(sstream);

          LOG.debug("Verifying Missing Paths for Merged Stream");

          if (filesList.size() > 0) {
            PublishMissingPathsTest.VerifyMissingPublishPaths(fs,
                todaysdate.getTime(), behinddate,
                this.destinationCluster.getFinalDestDirRoot()
                + sstream);

            String commitpath = destinationCluster.getFinalDestDirRoot()
                + sstream;          

            LOG.debug("Verifying Merged Paths in Stream for directory "
                + commitpath);
            List<String> commitPaths = new ArrayList<String>();
            FileStatus lastFile = getAllFiles(new Path(commitpath), fs,
                commitPaths);

            try {
              LOG.debug("Checking in Path for Merged mapred Output, No. of files: "
                  + commitPaths.size());

              /*
               * Last minute files wont be processed by MergedStreamService
               * Here MergedStreamService process all files as last minute dir
               * is empty.
               */
              for (int j = 0; j < filesList.size(); ++j) {
                String checkpath = filesList.get(j);
                LOG.debug("Merged Checking file: " + checkpath);
                Assert.assertTrue(commitPaths.contains(checkpath));
                totalFileProcessedInRun++;
              }
            } catch (NumberFormatException e) {
            }

          }
        }
      }
      // verfying audit is generated for all the messages
      MockInMemoryPublisher mPublisher = (MockInMemoryPublisher) Databus.getPublisher();
      BlockingQueue<Message> auditQueue = mPublisher.source
          .get(AuditUtil.AUDIT_STREAM_TOPIC_NAME);
      Message tmpMsg;
      int auditReceived = 0;
      while ((tmpMsg = auditQueue.poll()) != null) {
        byte[] auditData = tmpMsg.getData().array();
        TDeserializer deserializer = new TDeserializer();
        AuditMessage msg = new AuditMessage();
        deserializer.deserialize(msg, auditData);
        auditReceived += msg.getReceivedSize();
      }
      /*
       * Number of counters for each file is 2 as we have created the messages
       * with two different timestamps(falls in different window) in the file.
       * Counter name is func(streamname, filename, timestamp)
       */
      Assert.assertEquals(auditReceived, totalFileProcessedInRun * 2);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(
          "Error in MergedStreamService Test PostExecute");
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

  public void testRequalification() throws Exception {
    Path p = new Path("hdfs://xxxx:1000/abc/abc");
    String readUrl = srcCluster.getReadUrl();
    Path expectedPath = new Path(readUrl, "/abc/abc");
    Path path = this.fullyQualifyCheckPointWithReadURL(p, srcCluster);
    LOG.info("Expected Requalified path is " + expectedPath);
    Assert.assertEquals(expectedPath, path);
  }

  @Override
  public Cluster getCluster() {
    return destinationCluster;
  }

  public FileSystem getFileSystem() {
    return fs;
  }

}
