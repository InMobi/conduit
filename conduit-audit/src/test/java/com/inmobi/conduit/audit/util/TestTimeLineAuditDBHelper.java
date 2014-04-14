package com.inmobi.conduit.audit.util;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.inmobi.conduit.audit.Filter;
import com.inmobi.conduit.audit.GroupBy;
import com.inmobi.conduit.audit.LatencyColumns;
import com.inmobi.conduit.audit.Tuple;
import com.inmobi.messaging.ClientConfig;

public class TestTimeLineAuditDBHelper extends AuditDBUtil {

  protected Date fromDate = new Date(1388534400000l);
  protected Date toDate = new Date(1391212800000l);
  protected Date dateInbetween = new Date(1388534460000l);
  protected Date dateInbetweenDifferntDay = new Date(1388620860000l);
  private Tuple t1;
  private Tuple t2;
  private Tuple t3;
  private Set<Tuple> tupleSet = new HashSet<Tuple>();

  @BeforeClass
  public void setup() {

    Map<LatencyColumns, Long> latencyCountMap1 =
        new HashMap<LatencyColumns, Long>();
    Map<LatencyColumns, Long> latencyCountMap2 =
        new HashMap<LatencyColumns, Long>();
    Map<LatencyColumns, Long> latencyCountMap3 =
        new HashMap<LatencyColumns, Long>();
    latencyCountMap1.put(LatencyColumns.C1, 5l);
    latencyCountMap2.put(LatencyColumns.C1, 50l);
    latencyCountMap3.put(LatencyColumns.C1, 500l);
    t1 =
        new Tuple("localhost", "tier1", "cluster1", fromDate, "topic1",
            latencyCountMap1, 100l);
    t2 =
        new Tuple("localhost", "tier1", "cluster1", dateInbetween, "topic2",
            latencyCountMap2, 10l);

    t3 =
        new Tuple("localhost", "tier1", "cluster1", dateInbetweenDifferntDay,
            "topic2", latencyCountMap2, 1l);
    tupleSet.add(t1);
    tupleSet.add(t2);
    tupleSet.add(t3);

  }

  @AfterClass
  public void shutDown() {
    super.shutDown();
  }

  @Test
  public void testRetrieveDefaultTimeBucket() {

    ClientConfig conf =
        ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE);
    setupDB(false);
    TimeLineAuditDBHelper helper = new TimeLineAuditDBHelper(conf, null, null);
    helper.update(tupleSet);

    GroupBy groupBy = new GroupBy("TIER,HOSTNAME,CLUSTER,TIMEINTERVAL");
    Filter filter = new Filter("");
    Set<Tuple> tupleSet = helper.retrieve(toDate, fromDate, filter, groupBy);
    Assert.assertEquals(2, tupleSet.size());
    Iterator<Tuple> tupleSetIter = tupleSet.iterator();
    Assert.assertTrue(tupleSetIter.hasNext());
    Tuple returnedTuple = tupleSetIter.next();
    Assert.assertEquals(t3.getHostname(), returnedTuple.getHostname());
    Assert.assertEquals(t3.getTier(), returnedTuple.getTier());
    Assert.assertEquals(null, returnedTuple.getTopic());
    Assert.assertEquals(t3.getSent(), returnedTuple.getSent());
    for (LatencyColumns latencyColumns : LatencyColumns.values()) {
      Long val1 = t3.getLatencyCountMap().get(latencyColumns);
      if (val1 == null)
        val1 = 0l;

      Long val4 = returnedTuple.getLatencyCountMap().get(latencyColumns);
      if (val4 == null)
        val4 = 0l;
      Long valx = val1;
      Assert.assertEquals(valx, val4);
    }

    Assert.assertTrue(tupleSetIter.hasNext());
    returnedTuple = tupleSetIter.next();
    Assert.assertEquals(t1.getHostname(), returnedTuple.getHostname());
    Assert.assertEquals(t1.getTier(), returnedTuple.getTier());
    Assert.assertEquals(null, returnedTuple.getTopic());
    Assert.assertEquals(t1.getSent() + t2.getSent(), returnedTuple.getSent());
    for (LatencyColumns latencyColumns : LatencyColumns.values()) {
      Long val1 = t1.getLatencyCountMap().get(latencyColumns);
      if (val1 == null)
        val1 = 0l;
      Long val2 = t2.getLatencyCountMap().get(latencyColumns);
      if (val2 == null)
        val2 = 0l;

      Long val4 = returnedTuple.getLatencyCountMap().get(latencyColumns);
      if (val4 == null)
        val4 = 0l;
      Long valx = val1 + val2;
      Assert.assertEquals(valx, val4);
    }

  }

  @Test
  public void testRetrieveGroupByOneMin() {

    ClientConfig conf =
        ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE);
    conf.set(TimeLineAuditDBHelper.TIMEBUCKET, "1");
    setupDB(false);
    TimeLineAuditDBHelper helper = new TimeLineAuditDBHelper(conf, null, null);
    helper.update(tupleSet);

    GroupBy groupBy = new GroupBy("TIER,HOSTNAME,CLUSTER,TIMEINTERVAL");
    Filter filter = new Filter("");
    Set<Tuple> tupleSet = helper.retrieve(toDate, fromDate, filter, groupBy);
    Assert.assertEquals(3, tupleSet.size());
    Iterator<Tuple> tupleSetIter = tupleSet.iterator();
    Assert.assertTrue(tupleSetIter.hasNext());
    Tuple returnedTuple = tupleSetIter.next();
    Assert.assertEquals(t2.getHostname(), returnedTuple.getHostname());
    Assert.assertEquals(t2.getTier(), returnedTuple.getTier());
    Assert.assertEquals(null, returnedTuple.getTopic());
    Assert.assertEquals(t2.getSent(), returnedTuple.getSent());
    for (LatencyColumns latencyColumns : LatencyColumns.values()) {
      Long val1 = t2.getLatencyCountMap().get(latencyColumns);
      if (val1 == null)
        val1 = 0l;

      Long val4 = returnedTuple.getLatencyCountMap().get(latencyColumns);
      if (val4 == null)
        val4 = 0l;
      Long valx = val1;
      Assert.assertEquals(valx, val4);
    }

    Assert.assertTrue(tupleSetIter.hasNext());
    returnedTuple = tupleSetIter.next();
    Assert.assertEquals(t3.getHostname(), returnedTuple.getHostname());
    Assert.assertEquals(t3.getTier(), returnedTuple.getTier());
    Assert.assertEquals(null, returnedTuple.getTopic());
    Assert.assertEquals(t3.getSent(), returnedTuple.getSent());
    for (LatencyColumns latencyColumns : LatencyColumns.values()) {
      Long val1 = t3.getLatencyCountMap().get(latencyColumns);
      if (val1 == null)
        val1 = 0l;

      Long val4 = returnedTuple.getLatencyCountMap().get(latencyColumns);
      if (val4 == null)
        val4 = 0l;
      Long valx = val1;
      Assert.assertEquals(valx, val4);
    }

    Assert.assertTrue(tupleSetIter.hasNext());
    returnedTuple = tupleSetIter.next();
    Assert.assertEquals(t1.getHostname(), returnedTuple.getHostname());
    Assert.assertEquals(t1.getTier(), returnedTuple.getTier());
    Assert.assertEquals(null, returnedTuple.getTopic());
    Assert.assertEquals(t1.getSent(), returnedTuple.getSent());
    for (LatencyColumns latencyColumns : LatencyColumns.values()) {
      Long val1 = t1.getLatencyCountMap().get(latencyColumns);
      if (val1 == null)
        val1 = 0l;

      Long val4 = returnedTuple.getLatencyCountMap().get(latencyColumns);
      if (val4 == null)
        val4 = 0l;
      Long valx = val1;
      Assert.assertEquals(valx, val4);
    }

  }

  @Test
  public void testTimeGroup() {
    ClientConfig conf =
        ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE);

    //TimeRange of query < 3 days
    TimeLineAuditDBHelper helper = new TimeLineAuditDBHelper(conf,
        "01-01-2014-00:00", "03-01-2014-00:00");
    Assert.assertEquals(60 * 60000, helper.getTimeGroup());

    //TimeRange of query > 3 days but < 1 week
    helper = new TimeLineAuditDBHelper(conf, "01-01-2014-00:00",
        "06-01-2014-00:00");
    Assert.assertEquals(120 * 60000, helper.getTimeGroup());

    //TimeRange of query > 1 week but < 2 week
    helper = new TimeLineAuditDBHelper(conf, "01-01-2014-00:00",
        "13-01-2014-00:00");
    Assert.assertEquals(240 * 60000, helper.getTimeGroup());

    //TimeRange of query > 2 weeks but < 3 weeks
    helper = new TimeLineAuditDBHelper(conf, "01-01-2014-00:00",
        "20-01-2014-00:00");
    Assert.assertEquals(360 * 60000, helper.getTimeGroup());

    //TimeRange of query > 3 weeks but < 4 weeks
    helper = new TimeLineAuditDBHelper(conf, "01-01-2014-00:00",
        "27-01-2014-00:00");
    Assert.assertEquals(480 * 60000, helper.getTimeGroup());

    //TimeRange of query > 4 weeks but < 8 weeks
    helper = new TimeLineAuditDBHelper(conf, "01-01-2014-00:00",
        "15-02-2014-00:00");
    Assert.assertEquals(720 * 60000, helper.getTimeGroup());

    //TimeRange of query > 8 weeks
    helper = new TimeLineAuditDBHelper(conf, "01-01-2014-00:00",
        "03-03-2014-00:00");
    Assert.assertEquals(1440 * 60000, helper.getTimeGroup());

    //Default time is less than timeinterval bucket set in conf
    conf.set(TimeLineAuditDBHelper.TIMEBUCKET, "180");
    helper = new TimeLineAuditDBHelper(conf, "01-01-2014-00:00",
        "06-01-2014-00:00");
    Assert.assertEquals(180 * 60000, helper.getTimeGroup());

    //Default time is greater than timeinterval bucket set in conf
    helper = new TimeLineAuditDBHelper(conf, "01-01-2014-00:00",
        "23-01-2014-00:00");
    Assert.assertEquals(480 * 60000, helper.getTimeGroup());
  }

}
