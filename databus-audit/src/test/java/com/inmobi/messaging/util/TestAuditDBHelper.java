package com.inmobi.messaging.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Set;

import junit.framework.Assert;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.inmobi.databus.audit.AuditStats;
import com.inmobi.databus.audit.Filter;
import com.inmobi.databus.audit.GroupBy;
import com.inmobi.databus.audit.LatencyColumns;
import com.inmobi.databus.audit.Tuple;
import com.inmobi.databus.audit.util.AuditDBConstants;
import com.inmobi.databus.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;

public class TestAuditDBHelper extends  AuditDBUtil {

  @BeforeClass
  public void setup() {
    setupDB(false);
  }

  @AfterClass
  public void shutDown() {
    super.shutDown();
  }

  @Test(priority = 1)
  public void testUpdate() {
    ClientConfig config = ClientConfig.loadFromClasspath(AuditStats.CONF_FILE);
    AuditDBHelper helper = new AuditDBHelper(config);
    String selectStmt = helper.getSelectStmtForUpdation();
    PreparedStatement selectStatement = null;
    ResultSet rs = null;
    try {
      selectStatement = connection.prepareStatement(selectStmt);
      rs = getResultSetOfQuery(selectStatement, tuple1);
      Assert.assertNotNull(rs);
      Assert.assertFalse(rs.next());
      Assert.assertTrue(helper.update(tupleSet1));
      rs = getResultSetOfQuery(selectStatement, tuple1);
      Assert.assertNotNull(rs);
      Assert.assertTrue(rs.next());
      Assert.assertEquals(tuple1.getSent(), rs.getLong(AuditDBConstants.SENT));
      for (LatencyColumns latencyColumns : LatencyColumns.values()) {
        Long val = tuple1.getLatencyCountMap().get(latencyColumns);
        if (val == null)
          val = 0l;
        Assert.assertEquals(val, (Long) rs.getLong(latencyColumns.toString()));
      }
      Assert.assertEquals(tuple1.getLostCount(),
          (Long) rs.getLong(LatencyColumns.C600.toString()));
      Assert.assertTrue(helper.update(tupleSet2));
      rs = getResultSetOfQuery(selectStatement, tuple1);
      Assert.assertNotNull(rs);
      Assert.assertTrue(rs.next());
      Assert.assertEquals(tuple1.getSent() + tuple2.getSent(),
          rs.getLong(AuditDBConstants.SENT));
      for (LatencyColumns latencyColumns : LatencyColumns.values()) {
        Long val1 = tuple1.getLatencyCountMap().get(latencyColumns);
        if (val1 == null)
          val1 = 0l;
        Long val2 = tuple2.getLatencyCountMap().get(latencyColumns);
        if (val2 == null)
          val2 = 0l;
        Assert.assertEquals(val1 + val2, rs.getLong(latencyColumns
            .toString()));
      }
      Assert.assertEquals(tuple1.getLostCount() + tuple2.getLostCount(),
          rs.getLong(LatencyColumns.C600.toString()));
      Assert.assertTrue(helper.update(tupleSet3));
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (selectStatement != null) {
          selectStatement.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  private ResultSet getResultSetOfQuery(PreparedStatement selectStatement,
                                        Tuple tuple) {
    int index = 1;
    try {
      selectStatement.setLong(index++, tuple.getTimestamp().getTime());
      selectStatement.setString(index++, tuple.getHostname());
      selectStatement.setString(index++, tuple.getTopic());
      selectStatement.setString(index++, tuple.getTier());
      selectStatement.setString(index++, tuple.getCluster());
      return selectStatement.executeQuery();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Test(priority = 2)
  public void testRetrieve() {
    GroupBy groupBy = new GroupBy("TIER,HOSTNAME,CLUSTER");
    Filter filter = new Filter("hostname="+tuple1.getHostname());
    AuditDBHelper helper = new AuditDBHelper(
        ClientConfig.loadFromClasspath(AuditStats.CONF_FILE));
    Set<Tuple> tupleSet = helper.retrieve(toDate, fromDate, filter, groupBy);
    Assert.assertEquals(1, tupleSet.size());
    Iterator<Tuple> tupleSetIter = tupleSet.iterator();
    Assert.assertTrue(tupleSetIter.hasNext());
    Tuple returnedTuple = tupleSetIter.next();
    Assert.assertEquals(tuple1.getHostname(), returnedTuple.getHostname());
    Assert.assertEquals(tuple1.getTier(), returnedTuple.getTier());
    Assert.assertEquals(null, returnedTuple.getTopic());
    Assert.assertEquals(tuple1.getSent() + tuple2.getSent() + tuple3.getSent(),
        returnedTuple.getSent());
    for (LatencyColumns latencyColumns : LatencyColumns.values()) {
      Long val1 = tuple1.getLatencyCountMap().get(latencyColumns);
      if (val1 == null)
        val1 = 0l;
      Long val2 = tuple2.getLatencyCountMap().get(latencyColumns);
      if (val2 == null)
        val2 = 0l;
      Long val3 = tuple3.getLatencyCountMap().get(latencyColumns);
      if (val3 == null)
        val3 = 0l;
      Long val4 = returnedTuple.getLatencyCountMap().get(latencyColumns);
      if (val4 == null)
        val4 = 0l;
      Long valx = val1 + val2 + val3;
      Assert.assertEquals(valx, val4);
    }
    Assert.assertEquals((Long) (tuple1.getLostCount() + tuple2.getLostCount() +
        tuple3.getLostCount()), returnedTuple.getLostCount());
    filter = new Filter(null);
    tupleSet = helper.retrieve(toDate, fromDate, filter, groupBy);
    Assert.assertEquals(2, tupleSet.size());
  }
}
