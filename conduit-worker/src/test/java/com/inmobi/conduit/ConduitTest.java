package com.inmobi.conduit;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import com.inmobi.conduit.distcp.MergeMirrorStreamTest;
import com.inmobi.conduit.distcp.MirrorStreamService;
import com.inmobi.conduit.distcp.TestMergedStreamService;
import com.inmobi.conduit.distcp.TestMirrorStreamService;
import com.inmobi.conduit.local.LocalStreamService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.conduit.distcp.MergedStreamService;
import com.inmobi.conduit.local.TestLocalStreamService;

public class ConduitTest extends TestMiniClusterUtil {

  private static final Log LOG = LogFactory.getLog(MergeMirrorStreamTest.class);

  // @BeforeSuite
  public void setup() throws Exception {
    // clean up the test data if any thing is left in the previous runs
    cleanup();
    super.setup(2, 2, 1);
  }

  // @AfterSuite
  public void cleanup() throws Exception {
    super.cleanup();
  }

  public static class ConduitServiceTest extends Conduit {
    public ConduitServiceTest(ConduitConfig config,
                              Set<String> clustersToProcess) {
      super(config, clustersToProcess);
    }

    @Override
    protected LocalStreamService getLocalStreamService(ConduitConfig config,
        Cluster cluster, Cluster currentCluster, Set<String> streamsToProcess) throws IOException {
      return new TestLocalStreamService(config, cluster, currentCluster,
          new FSCheckpointProvider(cluster.getCheckpointDir()),
          streamsToProcess, null);
    }

    @Override
    protected MergedStreamService getMergedStreamService(ConduitConfig config,
        Cluster srcCluster, Cluster dstCluster, Cluster currentCluster,
        Set<String> streamsToProcess)
        throws Exception {

      return new TestMergedStreamService(config, srcCluster, dstCluster,
          currentCluster, streamsToProcess, null);
    }

    @Override
    protected MirrorStreamService getMirrorStreamService(ConduitConfig config,
        Cluster srcCluster, Cluster dstCluster, Cluster currentCluster,
        Set<String> streamsToProcess)
        throws Exception {
      return new TestMirrorStreamService(config, srcCluster, dstCluster,
          currentCluster, streamsToProcess, null);
    }

  }

  private static ConduitServiceTest testService = null;

  // @Test
  public void testConduit() throws Exception {
    testConduit("testConduitService_simple.xml");
  }

  private void testConduit(String filename) throws Exception {
    ConduitConfigParser configParser = new ConduitConfigParser(filename);
    ConduitConfig config = configParser.getConfig();
    Set<String> clustersToProcess = new HashSet<String>();
    FileSystem fs = FileSystem.getLocal(new Configuration());

    for (Map.Entry<String, Cluster> cluster : config.getClusters().entrySet()) {
      String jobTracker = super.CreateJobConf().get("mapred.job.tracker");
      cluster.getValue().getHadoopConf().set("mapred.job.tracker", jobTracker);
    }

    for (Map.Entry<String, SourceStream> sstream : config.getSourceStreams()
        .entrySet()) {
      clustersToProcess.addAll(sstream.getValue().getSourceClusters());
    }

    testService = new ConduitServiceTest(config, clustersToProcess);

    Timer timer = new Timer();
    Calendar calendar = new GregorianCalendar();
    calendar.add(Calendar.MINUTE, 5);

    timer.schedule(new TimerTask() {
      public void run() {
        try {
          LOG.info("Stopping Conduit Test Service");
          testService.stop();
          LOG.info("Done stopping Conduit Test Service");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, calendar.getTime());

    LOG.info("Starting Conduit Test Service");
    testService.startConduit();

    for (Map.Entry<String, Cluster> cluster : config.getClusters().entrySet()) {
      fs.delete(new Path(cluster.getValue().getRootDir()), true);
    }

    LOG.info("Done with Conduit Test Service");
  }

}
