package com.inmobi.databus.audit;

import com.inmobi.messaging.consumer.audit.GroupBy;
import com.inmobi.messaging.util.AuditDBUtil;
import com.inmobi.messaging.util.AuditUtil;
import junit.framework.Assert;
import org.testng.annotations.*;

import java.text.SimpleDateFormat;
import java.util.*;

public class TestAuditDBQuery extends AuditDBUtil {

  SimpleDateFormat formatter = new SimpleDateFormat(AuditUtil.DATE_FORMAT);

  @BeforeClass
  public void setup() {
    setupDB(true);
    formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  @AfterClass
  public void shutDown() {
    super.shutDown();
  }

  @Test
  public void testPercentileListNotNull()  {
    Date fromDate = new Date();
    Date toDate = new Date(fromDate.getTime()+60000000l);
    AuditDbQuery query = new AuditDbQuery(formatter.format(toDate),
        formatter.format(fromDate), "", "TIER,TOPIC,HOSTNAME", null, "95,99," +
        "99.9,99.99");
    Set<Float> expectedSet = new HashSet<Float>();
    expectedSet.add(95.00f);
    expectedSet.add(99.00f);
    expectedSet.add(99.90f);
    expectedSet.add(99.99f);
    try {
      query.parseAndSetArguments();
    } catch (Exception e) {
      e.printStackTrace();
    }
    Assert.assertTrue(query.percentileSet != null);
    Assert.assertEquals(4, query.percentileSet.size());
    Assert.assertTrue(query.percentileSet.containsAll(expectedSet));
  }

  @Test
  public void testPercentileSetIsNull() {
    Date fromDate = new Date();
    Date toDate = new Date(fromDate.getTime()+60000000l);
    AuditDbQuery query = new AuditDbQuery(formatter.format(toDate),
        formatter.format(fromDate), "", "TIER,TOPIC,HOSTNAME", null);
    try {
      query.parseAndSetArguments();
    } catch (Exception e) {
      e.printStackTrace();
    }
    Assert.assertTrue(query.percentileSet == null);
  }

  @Test
  public void testReceivedAndSentStats() {
    String filterString = "hostname="+tuple1.getHostname();
    String groupByString = "HOSTNAME,CLUSTER,TIER";
    AuditDbQuery query = new AuditDbQuery(formatter.format(toDate),
        formatter.format(fromDate), filterString, groupByString, null);
    try {
      query.execute();
    } catch (Exception e) {
      e.printStackTrace();
    }
    Assert.assertEquals(1, query.getReceived().size());
    Assert.assertEquals(1, query.getSent().size());

    Map<Column, String> map = new HashMap<Column, String>();
    map.put(Column.HOSTNAME, tuple1.getHostname());
    map.put(Column.CLUSTER, tuple1.getCluster());
    map.put(Column.TIER, tuple1.getTier());
    GroupBy.Group grp = query.groupBy.getGroup(map);
    Assert.assertEquals((Long) (tuple1.getReceived() + tuple2.getReceived() +
        tuple3.getReceived()), query.getReceived().get(grp));
    Assert.assertEquals(
        (Long) (tuple1.getSent() + tuple2.getSent() + tuple3.getSent()),
        query.getSent().get(grp));
  }

  @Test
  public void testTupleSetRetured() {
    String filterString = "";
    String groupByString = "HOSTNAME,CLUSTER,TIER";
    AuditDbQuery query = new AuditDbQuery(formatter.format(toDate),
        formatter.format(fromDate), filterString, groupByString, null);
    try {
      query.execute();
    } catch (Exception e) {
      e.printStackTrace();
    }
    Assert.assertEquals(2, query.getTupleSet().size());
    for( Tuple returnedTuple : query.getTupleSet()) {
      if(returnedTuple.getHostname().equals(tuple1.getHostname())) {
        Assert.assertEquals((tuple1.getReceived() + tuple2.getReceived() +
            tuple3.getReceived()), returnedTuple.getReceived());
        Assert.assertEquals((tuple1.getSent() + tuple2.getSent() + tuple3.getSent()),
            returnedTuple.getSent());
        Assert.assertEquals((Long) (tuple1.getLostCount() + tuple2.getLostCount
            () + tuple3.getLostCount()), returnedTuple.getLostCount());
      } else if(returnedTuple.getHostname().equals(tuple4.getHostname())) {
        Assert.assertEquals(tuple4.getReceived(), returnedTuple.getReceived());
        Assert.assertEquals(tuple4.getSent(), returnedTuple.getSent());
        Assert.assertEquals(tuple4.getLostCount(), returnedTuple.getLostCount());
      }
    }
  }

  @Test
  public void testGroupOfTuplesReturned() {
    String filterString = "";
    String groupByString;
    AuditDbQuery query;
    try {
      groupByString = "HOSTNAME,CLUSTER,TIER";
      query = new AuditDbQuery(formatter.format(toDate),
          formatter.format(fromDate), filterString, groupByString, null);
      query.execute();
      tuple1.setGroupBy(query.groupBy);
      tuple3.setGroupBy(query.groupBy);
      tuple4.setGroupBy(query.groupBy);
      GroupBy.Group grp1 = tuple1.getGroup();
      GroupBy.Group grp2 = tuple3.getGroup();
      GroupBy.Group grp3 = tuple4.getGroup();
      Assert.assertTrue(grp1.equals(grp2));
      Assert.assertFalse(grp1.equals(grp3));
      Assert.assertEquals(2, query.getTupleSet().size());
      for (Tuple returnedTuple : query.getTupleSet()) {
        Assert.assertNull(returnedTuple.getTopic());
        Assert.assertNotNull(returnedTuple.getHostname());
        Assert.assertNotNull(returnedTuple.getTier());
        Assert.assertNotNull(returnedTuple.getCluster());
        returnedTuple.setGroupBy(query.groupBy);
        Assert.assertTrue( returnedTuple.getGroup().equals(grp1) ||
            returnedTuple.getGroup().equals(grp3));
      }

      groupByString = "HOSTNAME,CLUSTER,TIER,TOPIC";
      query = new AuditDbQuery(formatter.format(toDate),
          formatter.format(fromDate), filterString, groupByString, null);
      query.execute();
      tuple1.setGroupBy(query.groupBy);
      tuple3.setGroupBy(query.groupBy);
      tuple4.setGroupBy(query.groupBy);
      grp1 = tuple1.getGroup();
      grp2 = tuple3.getGroup();
      grp3 = tuple4.getGroup();
      Assert.assertFalse(grp1.equals(grp2));
      Assert.assertFalse(grp1.equals(grp3));
      Assert.assertEquals(3, query.getTupleSet().size());
      for (Tuple returnedTuple : query.getTupleSet()) {
        Assert.assertNotNull(returnedTuple.getTopic());
        Assert.assertNotNull(returnedTuple.getHostname());
        Assert.assertNotNull(returnedTuple.getTier());
        Assert.assertNotNull(returnedTuple.getCluster());
        returnedTuple.setGroupBy(query.groupBy);
        Assert.assertTrue(returnedTuple.getGroup().equals(grp1) ||
            returnedTuple.getGroup().equals(grp2) ||
            returnedTuple.getGroup().equals(grp3));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Test
  public void testFilter() {
    String groupByString = "CLUSTER,HOSTNAME,TIER,TOPIC";
    String filterString;
    AuditDbQuery query;
    try {
      filterString = Column.HOSTNAME + "=" + tuple3.getHostname() + "," +
          "" + Column.TOPIC + "=" + tuple3.getTopic();
      query = new AuditDbQuery(formatter.format(toDate),
          formatter.format(fromDate), filterString, groupByString, null);
      query.execute();
      Assert.assertEquals(1, query.getTupleSet().size());

      filterString = Column.HOSTNAME + "=" + tuple1.getHostname();
      query = new AuditDbQuery(formatter.format(toDate),
          formatter.format(fromDate), filterString, groupByString, null);
      query.execute();
      Assert.assertEquals(2, query.getTupleSet().size());

      filterString = Column.TOPIC + "=" + tuple1.getTopic();
      query = new AuditDbQuery(formatter.format(toDate),
          formatter.format(fromDate), filterString, groupByString, null);
      query.execute();
      Assert.assertEquals(2, query.getTupleSet().size());

      filterString = Column.TIER + "=" + Tier.AGENT.toString();
      query = new AuditDbQuery(formatter.format(toDate),
          formatter.format(fromDate), filterString, groupByString, null);
      query.execute();
      Assert.assertEquals(3, query.getTupleSet().size());

      filterString = "";
      query = new AuditDbQuery(formatter.format(toDate),
          formatter.format(fromDate), filterString, groupByString, null);
      query.execute();
      Assert.assertEquals(3, query.getTupleSet().size());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testPercentile() {
    String groupByString = "CLUSTER,HOSTNAME,TIER";
    String filterString = "";
    String percentileString =  "80,90,95,99,99.9";
    AuditDbQuery query;
    try {
      query = new AuditDbQuery(formatter.format(toDate),
          formatter.format(fromDate), filterString, groupByString, null,
          percentileString);
      query.execute();
      Assert.assertEquals(2, query.getPercentile().size());
      for (Map.Entry<Tuple, Map<Float, Integer>> entry : query.getPercentile
          ().entrySet()) {
        Assert.assertEquals(5, entry.getValue().size());
        if (entry.getKey().getHostname().equals(tuple1.getHostname())) {
          Assert.assertEquals((Integer) LatencyColumns.C2.getValue(),
              entry.getValue().get(80.00f));
          Assert.assertEquals((Integer) LatencyColumns.C3.getValue(),
              entry.getValue().get(90.00f));
          Assert.assertEquals((Integer) LatencyColumns.C3.getValue(),
              entry.getValue().get(95.00f));
          Assert.assertEquals((Integer) LatencyColumns.C3.getValue(),
              entry.getValue().get(99.00f));
          Assert.assertEquals((Integer) LatencyColumns.C3.getValue(),
              entry.getValue().get(99.9f));
        } else if (entry.getKey().getHostname().equals(tuple4.getHostname())) {
          Assert.assertEquals((Integer) LatencyColumns.C1.getValue(),
              entry.getValue().get(80.00f));
          Assert.assertEquals((Integer) LatencyColumns.C1.getValue(),
              entry.getValue().get(90.00f));
          Assert.assertEquals((Integer) LatencyColumns.C1.getValue(),
              entry.getValue().get(95.00f));
          Assert.assertEquals((Integer) LatencyColumns.C1.getValue(),
              entry.getValue().get(99.00f));
          Assert.assertEquals((Integer) LatencyColumns.C1.getValue(),
              entry.getValue().get(99.9f));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}