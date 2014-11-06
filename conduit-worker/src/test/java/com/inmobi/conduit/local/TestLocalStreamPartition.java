package com.inmobi.conduit.local;

import java.io.IOException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.conduit.CheckpointProvider;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.distcp.MergeMirrorStreamPartitionTest;
import com.inmobi.conduit.utils.CalendarHelper;
import com.inmobi.conduit.utils.HCatPartitionComparator;

public class TestLocalStreamPartition extends TestLocalStreamService {

  private static final Log LOG = LogFactory.getLog(TestLocalStreamPartition.class);
  private String dbName;
  private Cluster srcCluster;
  private Set<String> streamsToProcess = new HashSet<String>();

  public TestLocalStreamPartition(ConduitConfig config, Cluster srcCluster,
      Cluster currentCluster, CheckpointProvider provider,
      Set<String> streamsToProcess) throws IOException {
    super(config, srcCluster, currentCluster, provider, streamsToProcess);
    this.streamsToProcess = streamsToProcess;
    this.srcCluster = srcCluster;
    dbName = Conduit.getHcatDBName();
  }

  @Override
  protected void postExecute() throws InterruptedException {
/*
    LOG.info("post execute in TestLocalStreamPartition");
    try {
     // hcatClient = getHCatClient();
      for (String stream : streamsToProcess) {
        String tableName = "conduit_local_" + stream;
        List<HCatPartition> list = hcatClient.getPartitions(dbName, tableName);
       // Collections.sort(list, new HCatPartitionComparator());
        Date lastAddedTime = MergeMirrorStreamPartitionTest.getLastAddedPartTime();
        Calendar cal = Calendar.getInstance();
        Date endTime = cal.getTime();
        Path localStreamPath = new Path(srcCluster.getLocalFinalDestDirRoot(), stream);
        Path startPath = CalendarHelper.getPathFromDate(lastAddedTime, localStreamPath);
        Path endPath = CalendarHelper.getPathFromDate(endTime, localStreamPath);
        LOG.info("Get local partitions from table : " + tableName + ", size:"+list.size());
        for (HCatPartition part : list) {
          LOG.info("Local partition location : " + part.getLocation());          
          Path path = new Path(part.getLocation());
          Assert.assertTrue(path.compareTo(startPath) >=0 && path.compareTo(endPath) <= 0);
        }
      }
    } catch (HCatException e) {
      LOG.info("Got exception while trying to get the partitions " + e.getCause());  
    } finally {
     // addToPool(hcatClient);
    }
*/  }
}
