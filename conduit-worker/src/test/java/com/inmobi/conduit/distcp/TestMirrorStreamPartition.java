package com.inmobi.conduit.distcp;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.common.HCatException;
import org.testng.Assert;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.HCatClientUtil;
import com.inmobi.conduit.utils.CalendarHelper;
import com.inmobi.conduit.utils.HCatPartitionComparator;

public class TestMirrorStreamPartition extends TestMirrorStreamService {
  private static final Log LOG = LogFactory.getLog(TestMirrorStreamPartition.class);

  private HCatClient hcatClient = null;
  private String dbName;
  private Set<String> streamsToProcess = new HashSet<String>();

  public TestMirrorStreamPartition(ConduitConfig config, Cluster srcCluster,
      Cluster destinationCluster, Cluster currentCluster,
      Set<String> streamsToProcess, HCatClientUtil hcatUtil) throws Exception {
    super(config, srcCluster, destinationCluster, currentCluster, streamsToProcess);
    this.streamsToProcess = streamsToProcess;
    dbName = Conduit.getHcatDBName();
  }

  @Override
  protected void postExecute() throws InterruptedException {

    LOG.info(" post execute in mirror stream parititon");
    try {
      //hcatClient = getHCatClient();
      for (String stream : streamsToProcess) {
        String tableName = "conduit_" + stream;
        List<HCatPartition> list = hcatClient.getPartitions(dbName, tableName);
        LOG.info("Get mirror partitions from table : " + tableName + ", size:"+list.size());
        //Collections.sort(list, new HCatPartitionComparator());
        Date lastAddedTime = MergeMirrorStreamPartitionTest.getLastAddedPartTime();
        Calendar cal = Calendar.getInstance();
        Date endTime = cal.getTime();
        Path mergeStreamPath = new Path(destCluster.getFinalDestDirRoot(), stream);
        Path startPath = CalendarHelper.getPathFromDate(lastAddedTime, mergeStreamPath);
        Path endPath = CalendarHelper.getPathFromDate(endTime, mergeStreamPath);

        for (HCatPartition part : list) {
          LOG.info("mirror partition location : " + part.getLocation());
          Path path = new Path(part.getLocation());
          Assert.assertTrue(path.compareTo(startPath) >=0 && path.compareTo(endPath) <= 0);          
        }
      }
    } catch (HCatException e) {
      LOG.info("Got exception while trying to get the partitions " + e.getCause());  
    } finally {
      //addToPool(hcatClient);
    }
  }
}