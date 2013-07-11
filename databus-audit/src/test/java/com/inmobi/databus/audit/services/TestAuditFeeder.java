package com.inmobi.databus.audit.services;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.databus.audit.*;
import com.inmobi.databus.audit.services.AuditFeederService.TupleKey;
import com.inmobi.databus.audit.util.AuditFeederTestUtil;
import com.inmobi.messaging.ClientConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class TestAuditFeeder extends AuditFeederTestUtil {

  @BeforeClass
  public void setup() {
    super.setup();
  }

  @Test
  public void testAddTuples() throws IOException {
    AuditFeederService feeder = new AuditFeederService(cluster,
        "emtpyRootDir", new ClientConfig());
    feeder.addTuples(msg1);
    TupleKey key1 = feeder.new TupleKey(new Date(upperRecieved1), tier1, topic,
        host, cluster);
    TupleKey key2 = feeder.new TupleKey(new Date(upperRecieved2), tier1, topic,
        host, cluster);
    Assert.assertFalse(key1.equals(key2));
    assert (feeder.tuples.size() == 2);
    assert (feeder.tuples.get(key1) != null);
    assert (feeder.tuples.get(key2) != null);
    assert (feeder.tuples.get(key1).getTimestamp().getTime() == upperRecieved1);
    assert (feeder.tuples.get(key2).getTimestamp().getTime() == upperRecieved2);
    assert (feeder.tuples.get(key1).getReceived() == 10);
    assert (feeder.tuples.get(key1).getSent() == 0);
    assert (feeder.tuples.get(key2).getReceived() == 15);
    assert (feeder.tuples.get(key2).getSent() == 12);
  }

  @Test
  public void testaddTupleDiffTiersSameHost() throws IOException {
    AuditFeederService feeder = new AuditFeederService(cluster, "emtpyRootDir",
        new ClientConfig());
    feeder.addTuples(msg1);
    feeder.addTuples(msg2);
    TupleKey key1 = feeder.new TupleKey(new Date(upperRecieved1), tier1, topic,
        host, cluster);
    TupleKey key2 = feeder.new TupleKey(new Date(upperRecieved2), tier1, topic,
        host, cluster);
    TupleKey key3 = feeder.new TupleKey(new Date(upperRecieved1), tier2, topic,
        host, cluster);
    Assert.assertFalse(key1.equals(key2));
    Assert.assertFalse(key1.equals(key3));
    assert (feeder.tuples.size() == 3);
    assert (feeder.tuples.get(key1) != null);
    assert (feeder.tuples.get(key2) != null);
    assert (feeder.tuples.get(key3) != null);
    assert (feeder.tuples.get(key1).getTimestamp().getTime() == upperRecieved1);
    assert (feeder.tuples.get(key2).getTimestamp().getTime() == upperRecieved2);
    assert (feeder.tuples.get(key3).getTimestamp().getTime() == upperRecieved1);
    assert (feeder.tuples.get(key1).getReceived() == 10);
    assert (feeder.tuples.get(key2).getReceived() == 15);
    assert (feeder.tuples.get(key3).getReceived() == 9);
    assert (feeder.tuples.get(key1).getLatencyCountMap().size() == 1);
    assert (feeder.tuples.get(key2).getLatencyCountMap().size() == 2);
    assert (feeder.tuples.get(key3).getLatencyCountMap().size() == 2);
    assert (feeder.tuples.get(key1).getSent() == 0);
    assert (feeder.tuples.get(key2).getSent() == 12);
    assert (feeder.tuples.get(key3).getSent() == 6);
  }

  @Test
  public void testMsgsAlignedAtMin() throws IOException {
    AuditFeederService feeder = new AuditFeederService(cluster, "emtpyRootDir",
        new ClientConfig());
    AuditMessage[] msgs = feeder.getAuditMessagesAlignedAtMinuteBoundary(msg1);
    assert (msgs.length == 2);
    assert (msgs[0].getReceived().get(received2) == 10);
    assert (msgs[0].getSent().get(received2) == 8);
    assert (msgs[1].getReceived().get(received2) == 5);
    assert (msgs[1].getReceived().get(received1) == 10);
    assert (msgs[1].getSent().get(received2) == 4);
  }

  @Test
  public void testExecute()
      throws IOException, InterruptedException, SQLException {
    setupPublisher();
    generateData(topic, totalData/2);
    generateData(topic1, totalData/2);
    publisher.close();
    setupAuditDB();
    AuditFeederService feeder = new AuditFeederServiceTest(cluster, "mock",
        ClientConfig.loadFromClasspath(AuditStats.CONF_FILE), publisher);
    feeder.execute();
    feeder.stop();
    int n = getNumberOfRowsInAuditDB();
    assert (n == 2);
    ResultSet rs = getAllRowsInAuditDB();
    assert(rs != null);
    assert(rs.next() == true);
    assert(rs.getString(Column.HOSTNAME.toString()).equalsIgnoreCase(
        InetAddress.getLocalHost().getHostName()));
    assert(rs.getString(Column.TIER.toString()).equalsIgnoreCase(
        Tier.PUBLISHER.toString()));
    assert(rs.getString(Column.TOPIC.toString()).equals(topic));
    assert(rs.getString(Column.CLUSTER.toString()).equals(cluster));
    assert(rs.getInt(LatencyColumns.C0.toString()) == totalData/2);
    assert(rs.next() == true);
    assert(rs.getString(Column.HOSTNAME.toString()).equalsIgnoreCase(
        InetAddress.getLocalHost().getHostName()));
    assert(rs.getString(Column.TIER.toString()).equalsIgnoreCase(
        Tier.PUBLISHER.toString()));
    assert(rs.getString(Column.TOPIC.toString()).equals(topic1));
    assert(rs.getString(Column.CLUSTER.toString()).equals(cluster));
    assert(rs.getInt(LatencyColumns.C0.toString()) == totalData/2);
    shutDownAuditDB();
    teardown();
  }

  @Test
  public void testConsumerDBUpdateFails()
      throws IOException, InterruptedException, SQLException {
    setupPublisher();
    generateData(topic, totalData / 2);
    Thread.sleep(30000);
    setupAuditDB();
    AuditFeederService feeder = new AuditFeederServiceTest(cluster, "mock",
        ClientConfig.loadFromClasspath(AuditStats.CONF_FILE), publisher);
    feeder.execute();
    addConstraintToAuditDB();
    generateData(topic1, totalData/2);
    publisher.close();
    feeder.execute();
    dropConstraintOfAuditDB();
    feeder.execute();
    feeder.stop();
    int n = getNumberOfRowsInAuditDB();
    assert (n == 2);
    ResultSet rs = getAllRowsInAuditDB();
    assert(rs != null);
    assert(rs.next() == true);
    assert(rs.getString(Column.HOSTNAME.toString()).equalsIgnoreCase(
        InetAddress.getLocalHost().getHostName()));
    assert(rs.getString(Column.TIER.toString()).equalsIgnoreCase(
        Tier.PUBLISHER.toString()));
    assert(rs.getString(Column.TOPIC.toString()).equals(topic));
    assert(rs.getString(Column.CLUSTER.toString()).equals(cluster));
    assert(rs.getInt(LatencyColumns.C0.toString()) == totalData);
    assert(rs.next() == true);
    assert(rs.getString(Column.HOSTNAME.toString()).equalsIgnoreCase(
        InetAddress.getLocalHost().getHostName()));
    assert(rs.getString(Column.TIER.toString()).equalsIgnoreCase(
        Tier.PUBLISHER.toString()));
    assert(rs.getString(Column.TOPIC.toString()).equals(topic1));
    assert(rs.getString(Column.CLUSTER.toString()).equals(cluster));
    assert(rs.getInt(LatencyColumns.C0.toString()) == totalData/2);
    shutDownAuditDB();
    teardown();
  }

}
