package com.inmobi.conduit.local;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;

import com.inmobi.conduit.AbstractService;
import com.inmobi.conduit.AbstractServiceTest;
import com.inmobi.conduit.CheckpointProvider;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConstants;
import com.inmobi.conduit.HCatClientUtil;
import com.inmobi.conduit.PublishMissingPathsTest;
import com.inmobi.conduit.SourceStream;
import com.inmobi.conduit.utils.CalendarHelper;
import com.inmobi.conduit.utils.FileUtil;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.testng.Assert;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.publisher.MockInMemoryPublisher;
import com.inmobi.messaging.util.AuditUtil;

public class TestLocalStreamService extends LocalStreamService implements
    AbstractServiceTest {
  private static Logger LOG = Logger.getLogger(TestLocalStreamService.class);
  private Cluster srcCluster = null;
  private CheckpointProvider provider = null;
  private static final int NUM_OF_FILES = 10;
  private List<String> tmpFilesList = null;
  private Map<String, List<String>> files = new HashMap<String, List<String>>();
  private Map<String, Set<String>> prevfiles = new HashMap<String, Set<String>>();
  private Calendar behinddate = null;
  private FileSystem fs = null;
  private Date todaysdate = null;
  public static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(5);
  }
  
  public static String getDateAsYYYYMMDDHHmm(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    return dateFormat.format(date);
  }

  public static List<String> createScribeData(FileSystem fs, String streamName,
      String pathName, int filesCount) throws Exception {
    
    Path createPath = new Path(pathName);
    fs.mkdirs(createPath);
    List<String> filesList = new ArrayList<String>();
    
    for (int j = 0; j < filesCount; ++j) {
      Thread.sleep(1000);
      String filenameStr = new String(streamName + "-"
          + getDateAsYYYYMMDDHHmm(new Date()) + "_" + idFormat.format(j));
      filesList.add(j, filenameStr);
      Path path = new Path(createPath, filesList.get(j));
      
      LOG.debug("Creating Test Data with filename [" + filesList.get(j) + "]");
      FSDataOutputStream streamout = fs.create(path);
      String content = "Creating Test data for teststream";
      /*
       * Here we are writing 3 messages to a file. Two messages were generated
       *  with same timestamp and genearating the 3rd message with a diff timestamp
       * such that it falls in another window as default window period is 60 sec.
       * So, we will be having two counters for each file.
       * CounterName is func(streamname, filename, timestamp)
       */
      Message msg = new Message(content.getBytes());
      long currentTimestamp = new Date().getTime();
      AuditUtil.attachHeaders(msg, currentTimestamp);
      byte[] encodeMsg = Base64.encodeBase64(msg.getData().array());
      // streamout.writeBytes("Creating Test data for teststream "
      // + filesList.get(j));
      // streamout
      // .writeBytes("AavN7wAAAUBPSdyVAAAEsAsAAQAAAA93ZWIxMDAyLmFkcy51YTIKAAIAAAFAT0nckQwAAwwAAQoAAU9J3JEBQByzCgAC1twAJZCWTZoADAADCAABAAABzwYAAgAMCAADAABxmAgABAAAm2kADAAECAABAAB5QAgAAgAAAAYGAAMABAsABAAAAAxOb2tpYTUxMzBjLTIABgAFAAEGAAYAAQgABwAAAAEKAAgAAAAAAAIp3AgACQAAAAEMAAsNAAQLCwAAAAACAAUADQAHCwsAAAAAAAYADAADDAAPCgABAAAAAAACKdwLAAIAAAAgNDAyOGNiZmYzYTZlYWY1NzAxM2E4OTA0MmZkYjAxY2UIAAMAAAABAA0AEAsLAAAAAQAAAAp1LWxvY2F0aW9uAAAAAlBLDQARCwsAAAACAAAAD3gtZm9yd2FyZGVkLWZvcgAAABk0Mi44My44Ni4xOSwgMTQxLjAuMTAuMjA3AAAAFHgtb3BlcmFtaW5pLXBob25lLXVhAAAADE5va2lhNTEzMGMtMgsAEgAAABZwci1TUEVDLUNUQVRBLTIwMTMwMTExDAATCAABAAAAAwgAAgAAAAMLAAMAAAACdWsLAAQAAAACdWsCAAUACwAGAAAACHNfc21hYXRvAAsAFAAAAAs0Mi44My44Ni4xOQsAFQAAABdodHRwOi8vd3d3LmFkaXF1aXR5LmNvbQgAFgAAAAULABoAAAAEYXhtbAsAGwAAACA0MDI4Y2JmZjNhNmVhZjU3MDEzYTg5MDQyZmRiMDFjZQ0AHQsLAAAAAQAAAAdyZWYtdGFnAAAACDY1ODEyNTIwCwAeAAAADE5va2lhNTEzMGMtMg0AIAsLAAAAAgAAAA5kLWxvY2FsaXphdGlvbgAAAAVlbl9QSwAAAAlkLW5ldHR5cGUAAAAHY2FycmllcgAPAAQMAAAAAAYABQABAgAGAAsABwAAAAJOTw8ACAsAAAAACwALAAAACzQyLjgzLjg2LjE5BgAMAAEGAA0AAAsADwAAACMzMy42NjY5OTk4MTY4OTQ1Myw3My4xMzMwMDMyMzQ4NjMyOA8AEQoAAAAACwASAAAADDMzLjcxLDczLjA4NggAFgAAChcEABc/hHrhR64UewwAGg8AAQgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==");
      streamout.write(encodeMsg);
      streamout.write("\n".getBytes());
      streamout.write(encodeMsg);
      streamout.write("\n".getBytes());
      // Genearate a msg with different timestamp.  Default window period is 60sec
      AuditUtil.attachHeaders(msg, currentTimestamp + 60001);
      encodeMsg = Base64.encodeBase64(msg.getData().array());
      streamout.write(encodeMsg);
      streamout.close();

      Assert.assertTrue(fs.exists(path));
    }
    
    return filesList;
  }
  
  public void doRecursiveListing(Path dir, Set<Path> listing,
  		FileSystem fs) throws IOException {

    FileStatus[] fileStatuses = null;
    try {
      fileStatuses = fs.listStatus(dir);
    } catch (FileNotFoundException e) {

    }
    if (fileStatuses == null || fileStatuses.length == 0) {
  		LOG.debug("No files in directory:" + dir);
  	} else {
  		for (FileStatus file : fileStatuses) {
  			if (file.isDir()) {
  				doRecursiveListing(file.getPath(), listing, fs);	
  			} else {
  				listing.add(file.getPath().getParent());
  			}
  		}
  	}
  }

  @Override
  protected void preExecute() throws Exception {
    try {
      // PublishMissingPathsTest.testPublishMissingPaths(this, true);
      behinddate = new GregorianCalendar();
      behinddate.add(Calendar.HOUR_OF_DAY, -2);
      
      for (Map.Entry<String, SourceStream> sstream : getConfig()
          .getSourceStreams().entrySet()) {
        
        Set<String> prevfilesList = new TreeSet<String>();
        String pathName = srcCluster.getDataDir() + File.separator
            + sstream.getValue().getName() + File.separator
            + srcCluster.getName() + File.separator;

        FileStatus[] fStats = null;
        try {
          fStats = fs.listStatus(new Path(pathName));
        } catch (FileNotFoundException e) {
          fStats = new FileStatus[0];
        }

        LOG.debug("Adding Previous Run Files in Path: " + pathName);
        for (FileStatus fStat : fStats) {
          LOG.debug("Previous File: " + fStat.getPath().getName());
          prevfilesList.add(fStat.getPath().getName());
        }
        
        List<String> filesList = createScribeData(fs, sstream.getValue()
          .getName(), pathName, NUM_OF_FILES);
        
        files.put(sstream.getValue().getName(), filesList);
        prevfiles.put(sstream.getValue().getName(), prevfilesList);
        
        String dummycommitpath = srcCluster.getLocalFinalDestDirRoot()
            + sstream.getValue().getName() + File.separator
            + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
        fs.mkdirs(new Path(dummycommitpath));
      }
      
      {
        LOG.debug("Creating Tmp Files for LocalStream");
        String pathName = srcCluster.getTmpPath() + File.separator
            + this.getName() + File.separator;
        tmpFilesList = createScribeData(fs, this.getName(), pathName, 1);
      }

      // Copy input format src jar to FS
      String inputFormatSrcJar = FileUtil
          .findContainingJar(com.inmobi.conduit.distcp.tools.mapred.UniformSizeInputFormat.class);
      fs.copyFromLocalFile(new Path(inputFormatSrcJar), inputFormatJarDestPath);
      // Copy AuditUtil src jar to FS
      String auditSrcJar = FileUtil.findContainingJar(
          com.inmobi.messaging.util.AuditUtil.class);
      fs.copyFromLocalFile(new Path(auditSrcJar), auditUtilJarDestPath);
    } catch (Exception e) {
      e.printStackTrace();
      throw new Error("Error in LocalStreamService Test PreExecute");
    } catch (AssertionError e) {
      e.printStackTrace();
      throw new Error("Error in LocalStreamService Test PreExecute");
    }
    todaysdate = new Date();
  }

  @Override
  protected void postExecute() throws Exception {
    try {
      int totalFileProcessedInRun = 0;
      for (Map.Entry<String, SourceStream> sstream : getConfig()
          .getSourceStreams().entrySet()) {
        
        LOG.debug("Verifying Tmp Files for LocalStream Stream "
            + sstream.getValue().getName());
        String pathName = srcCluster.getDataDir() + File.separator
            + sstream.getValue().getName() + File.separator
            + srcCluster.getName() + File.separator;
        for (int j = 0; j < tmpFilesList.size(); ++j) {
          Path tmppath = new Path(pathName + File.separator
              + tmpFilesList.get(j));
          Assert.assertFalse(fs.exists(tmppath));
        }
        
        LOG.info("Tmp Path does not exist for cluster " + srcCluster.getName());

        List<String> filesList = files.get(sstream.getValue().getName());
        Set<String> prevfilesList = prevfiles.get(sstream.getValue().getName());

        Path trashpath = srcCluster.getTrashPathWithDateHour();
        String streamPrefix = srcCluster.getLocalFinalDestDirRoot()
        		+ sstream.getValue().getName();
        Set<Path> listOfPaths = new HashSet<Path>();
        doRecursiveListing(new Path(streamPrefix), listOfPaths, fs);
        Path latestPath = null;
        for (Path path : listOfPaths) {
        	if (latestPath== null || (CalendarHelper.getDateFromStreamDir(new
              Path(streamPrefix), path).compareTo(CalendarHelper.getDateFromStreamDir
        					(new Path(streamPrefix), latestPath)) > 0)) {
        		latestPath = path;
        	}
        }

        // Make sure all the paths from dummy to mindir are created
        PublishMissingPathsTest.VerifyMissingPublishPaths(fs,
            todaysdate.getTime(), behinddate,
            srcCluster.getLocalFinalDestDirRoot() + sstream.getValue()
                .getName(), 180000);
  
        try {
          String streams_local_dir = latestPath + File.separator + srcCluster.getName(); 
        	LOG.debug("Checking in Path for mapred Output: " + streams_local_dir);
          
          // First check for the previous current file
          if (!prevfilesList.isEmpty()) {
            String prevcurrentFile = (String) prevfilesList.toArray()[prevfilesList
                .size() - 1];
            LOG.debug("Checking Data Replay in mapred Output");
            
            Iterator<String> prevFilesItr = prevfilesList.iterator();
            
            for (int i = 0; i < prevfilesList.size() - 1; ++i) {
              String dataReplayfile = prevFilesItr.next();
              LOG.debug("Checking Data Replay, File [" + dataReplayfile + "]");
              Assert.assertFalse(fs.exists(new Path(streams_local_dir + "-"
                  + dataReplayfile + ".gz")));
            }
            
            LOG.debug("Checking Previous Run Current file in mapred Output ["
                + prevcurrentFile + "]");
            Assert.assertTrue(fs.exists(new Path(streams_local_dir + "-"
                + prevcurrentFile + ".gz")));
            totalFileProcessedInRun++;
          }

          /*
           * When it is processing on limited num of files (throttling) in a run then
           * We should not check for all files as they may falls in different minute directories.
           * we Should check only files which were processed in the last run are present in the
           * latest/last directory of local stream.
           */
          int start = 0;
          String filesPerCollector = System.getProperty(ConduitConstants.FILES_PER_COLLECETOR_PER_LOCAL_STREAM);
          if (filesPerCollector != null) {
            start = ((NUM_OF_FILES - 1) / Integer.parseInt(filesPerCollector)) * Integer.parseInt(filesPerCollector);
            totalFileProcessedInRun += start;
          }
          for (int j = start; j < NUM_OF_FILES - 1; ++j) {
            LOG.debug("Checking file in mapred Output [" + filesList.get(j)
                + "]");
            Assert.assertTrue(fs.exists(new Path(streams_local_dir + "-"
                + filesList.get(j) + ".gz")));
            totalFileProcessedInRun++;
          }
          
          CheckpointProvider provider = this.getCheckpointProvider();
          
          String checkpoint = new String(provider.read(
              AbstractService.getCheckPointKey(getClass().getSimpleName(),
                  sstream.getValue().getName(), srcCluster.getName())));
          
          LOG.debug("Checkpoint for " + sstream.getValue().getName()
              + srcCluster.getName() + " is " + checkpoint);
          
          LOG.debug("Comparing Checkpoint " + checkpoint + " and "
              + filesList.get(NUM_OF_FILES - 2));
          Assert.assertTrue(checkpoint.compareTo(filesList
              .get(NUM_OF_FILES - 2)) == 0);
          
          LOG.debug("Verifying Collector Paths");
          
          Path collectorPath = new Path(srcCluster.getDataDir(), sstream
              .getValue().getName() + File.separator + srcCluster.getName());
          
          for (int j = NUM_OF_FILES - 7; j < NUM_OF_FILES; ++j) {
            LOG.debug("Verifying Collector Path " + collectorPath
                + " Previous File " + filesList.get(j));
            Assert.assertTrue(fs.exists(new Path(collectorPath, filesList
                .get(j))));
          }
          
          LOG.debug("Verifying Trash Paths");
          
          for (String trashfile : prevfilesList) {
            String trashfilename = srcCluster.getName() + "-" + trashfile;
            LOG.debug("Verifying Trash Path " + trashpath + " Previous File "
                + trashfilename);
            Assert.assertTrue(fs.exists(new Path(trashpath, trashfilename)));
          }
          
          // Here 6 is the number of files - trash paths which are excluded
          for (int j = 0; j < NUM_OF_FILES - 7; ++j) {
            if (filesList.get(j).compareTo(checkpoint) <= 0) {
              String trashfilename = srcCluster.getName() + "-"
                  + filesList.get(j);
              LOG.debug("Verifying Trash Path " + trashpath + "File "
                  + trashfilename);
              Assert.assertTrue(fs.exists(new Path(trashpath, trashfilename)));
            } else
              break;
          }
          


        } catch (NumberFormatException e) {
          
        }
        // Since merge will only pick the data if next minute directory is
        // present hence creating an empty next minute directory
        LOG.debug("Last path created in local stream is [" + latestPath + "]");
        Date lastPathDate = CalendarHelper.getDateFromStreamDir(new Path(
            streamPrefix), latestPath);
        Path nextPath = CalendarHelper.getNextMinutePathFromDate(lastPathDate,
            new Path(streamPrefix));
        LOG.debug("Creating empty path in local stream " + nextPath);
        fs.mkdirs(nextPath);
      }
      fs.delete(srcCluster.getTrashPathWithDateHour(), true);
      // verfying audit is generated for all the messages
      MockInMemoryPublisher mPublisher = (MockInMemoryPublisher) Conduit.getPublisher();
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
      /* audit won't be generated for last file as last file is not
       * processed by local stream
       * Number of counters for each file = 2 as we have created the messages
       * with two different timestamps(falls in different window) in the file
       */
      Assert.assertEquals(auditReceived, totalFileProcessedInRun * 2);
    } catch (Exception e) {
      e.printStackTrace();
      throw new Error("Error in LocalStreamService Test PostExecute");
    } catch (AssertionError e) {
      e.printStackTrace();
      throw new Error("Error in LocalStreamService Test PostExecute", e);
    }
    
  }
  
  public TestLocalStreamService(ConduitConfig config, Cluster srcCluster,
      Cluster currentCluster, CheckpointProvider provider,
      Set<String> streamsToProcess, HCatClientUtil hcatUtil)
          throws IOException {
    super(config, srcCluster, currentCluster, provider, streamsToProcess, hcatUtil);
    this.srcCluster = srcCluster;
    this.provider = provider;
    MessagePublisher publisher = MessagePublisherFactory.create();
    Conduit.setPublisher(publisher);
    try {
      this.fs = FileSystem.getLocal(new Configuration());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
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
  public Cluster getCluster() {
    return srcCluster;
  }

  public CheckpointProvider getCheckpointProvider() {
    return provider;
  }
  
  public FileSystem getFileSystem() {
    return fs;
  }

  public Map<String, Long> getLastProcessedMap() {
    return lastProcessedFile;
  }
}

