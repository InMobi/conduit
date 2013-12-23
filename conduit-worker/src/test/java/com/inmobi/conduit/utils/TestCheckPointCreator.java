package com.inmobi.conduit.utils;

import java.util.Date;

import com.inmobi.conduit.AbstractService;
import com.inmobi.conduit.CheckpointProvider;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.distcp.MirrorStreamService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.distcp.MergedStreamService;

public class TestCheckPointCreator {
  public static final String CLUSTER1 = "testcluster1";
  public static final String CLUSTER2 = "testcluster2";
  public static final String STREAM1 = "stream1";
  public static final String STREAM2 = "stream2";
  private ConduitConfigParser configParser;
  private ConduitConfig config;
  private Cluster cluster1, cluster2;
  private FileSystem fs1, fs2;

  @BeforeMethod
  public void setup() throws Exception {
    configParser = new ConduitConfigParser("test-mss-conduit1.xml");
    config = configParser.getConfig();
    cluster1 = config.getClusters().get(CLUSTER1);
    fs1 = FileSystem.get(cluster1.getHadoopConf());
    cluster2 = config.getClusters().get(CLUSTER2);
    fs2 = FileSystem.get(cluster2.getHadoopConf());
    fs1.delete(new Path(cluster1.getRootDir()), true);
    fs2.delete(new Path(cluster2.getRootDir()), true);
  }
  @Test
  public void testCheckPointMergeNoSourceProvided() throws Exception {
    Date date = new Date();
    Path p1 = new Path(cluster1.getLocalDestDir(STREAM1, date.getTime()));
    Path p2 = new Path(cluster2.getLocalDestDir(STREAM1, date.getTime()));
    fs1.create(p1);
    fs2.create(p2);
    CheckPointCreator creator = new CheckPointCreator(config, null, CLUSTER2,
        STREAM1, date);
    creator.createCheckPoint();
    String ck1 = AbstractService.getCheckPointKey(
        MergedStreamService.class.getSimpleName(), STREAM1, CLUSTER1);
    String ck2 = AbstractService.getCheckPointKey(
        MergedStreamService.class.getSimpleName(), STREAM1, CLUSTER2);
    assert (fs2.exists(new Path(cluster2.getCheckpointDir(),
 ck1 + ".ck")));
    assert (fs2.exists(new Path(cluster2.getCheckpointDir(),
 ck2 + ".ck")));
    CheckpointProvider provider = new FSCheckpointProvider(
        cluster2.getCheckpointDir());
    assert (new Path(new String(provider.read(ck1))).equals(p1));
    assert (new Path(new String(provider.read(ck2))).equals(p2));
  }

  @Test
  public void testCheckPointMirrorNoSourceProvided() throws Exception {
    Date date = new Date();
    Path p1 = new Path(cluster1.getFinalDestDir(STREAM2, date.getTime()));
    fs1.create(p1);
    CheckPointCreator creator = new CheckPointCreator(config, null, CLUSTER2,
        STREAM2, date);
    creator.createCheckPoint();
    String ck1 = AbstractService.getCheckPointKey(
        MirrorStreamService.class.getSimpleName(), STREAM2, CLUSTER1);
    String ck2 = AbstractService.getCheckPointKey(
        MirrorStreamService.class.getSimpleName(), STREAM2, CLUSTER2);
    assert (fs2.exists(new Path(cluster2.getCheckpointDir(), ck1 + ".ck")));
    assert (!fs2.exists(new Path(cluster2.getCheckpointDir(), ck2 + ".ck")));
    CheckpointProvider provider = new FSCheckpointProvider(
        cluster2.getCheckpointDir());
    assert (new Path(new String(provider.read(ck1))).equals(p1));
  }

  @Test
  public void testCheckPointMergeSourceProvided() throws Exception {
    Date date = new Date();
    Path p1 = new Path(cluster1.getLocalDestDir(STREAM1, date.getTime()));
    Path p2 = new Path(cluster2.getLocalDestDir(STREAM1, date.getTime()));
    System.out.println("creating path " + p1);
    fs1.create(p1);
    fs2.create(p2);
    CheckPointCreator creator = new CheckPointCreator(config, CLUSTER1,
        CLUSTER2, STREAM1, date);
    creator.createCheckPoint();
    String ck1 = AbstractService.getCheckPointKey(
        MergedStreamService.class.getSimpleName(), STREAM1, CLUSTER1);
    String ck2 = AbstractService.getCheckPointKey(
        MergedStreamService.class.getSimpleName(), STREAM1, CLUSTER2);
    assert (fs2.exists(new Path(cluster2.getCheckpointDir(), ck1 + ".ck")));
    assert (!fs2.exists(new Path(cluster2.getCheckpointDir(), ck2 + ".ck")));
    CheckpointProvider provider = new FSCheckpointProvider(
        cluster2.getCheckpointDir());
    assert (new Path(new String(provider.read(ck1))).equals(p1));
  }

  @Test
  public void testCheckPointMirrorSourceProvided() throws Exception {
    Date date = new Date();
    Path p1 = new Path(cluster1.getFinalDestDir(STREAM2, date.getTime()));
    fs1.create(p1);
    CheckPointCreator creator = new CheckPointCreator(config, CLUSTER1,
        CLUSTER2, STREAM2, date);
    creator.createCheckPoint();
    String ck1 = AbstractService.getCheckPointKey(
        MirrorStreamService.class.getSimpleName(), STREAM2, CLUSTER1);
    String ck2 = AbstractService.getCheckPointKey(
        MirrorStreamService.class.getSimpleName(), STREAM2, CLUSTER2);
    assert (fs2.exists(new Path(cluster2.getCheckpointDir(), ck1 + ".ck")));
    assert (!fs2.exists(new Path(cluster2.getCheckpointDir(), ck2 + ".ck")));
    CheckpointProvider provider = new FSCheckpointProvider(
        cluster2.getCheckpointDir());
    assert (new Path(new String(provider.read(ck1))).equals(p1));
  }

  @Test(expectedExceptions = { Exception.class })
  public void testCheckPointMirrorCheckPointPathDoesntExist() throws Exception {
    Date date = new Date();
    CheckPointCreator creator = new CheckPointCreator(config, null, CLUSTER2,
        STREAM2, date);
    creator.createCheckPoint();
  }
}
