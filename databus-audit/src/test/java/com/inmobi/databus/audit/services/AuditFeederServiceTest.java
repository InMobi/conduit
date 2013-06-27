package com.inmobi.databus.audit.services;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.testng.annotations.Test;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.databus.audit.services.AuditFeederService;
import com.inmobi.databus.audit.services.AuditFeederService.TupleKey;
import com.inmobi.messaging.ClientConfig;

public class AuditFeederServiceTest {

  @Test
  public void testAddTuples() throws IOException {
    String tier = "agent", host = "localhost", topic = "testTopic",cluster="testCluster";
    AuditMessage msg = new AuditMessage();
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.SECOND, 22);
    cal.set(Calendar.MILLISECOND, 7);
    long msgReceived = cal.getTimeInMillis();
    msg.setTimestamp(msgReceived);
    msg.setWindowSize(60);
    msg.setTopic(topic);
    msg.setTier(tier);
    msg.setHostname(host);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    long received1 = cal.getTimeInMillis();
    msg.putToReceived(received1, 10);
    cal.add(Calendar.MINUTE, -1);
    long received2 = cal.getTimeInMillis();
    msg.putToReceived(received2, 7);
    msg.putToSent(cal.getTimeInMillis(), 5);
    AuditFeederService feeder = new AuditFeederService(cluster,
        "emtpyRootDir", new ClientConfig());
    feeder.addTuples(msg);
    long upperRecieved1= received1+60*1000;
    long upperRecieved2= received2+60*1000;
    TupleKey key1 = feeder.new TupleKey(new Date(upperRecieved1), tier, topic,
        host, cluster);
    TupleKey key2 = feeder.new TupleKey(new Date(upperRecieved2), tier, topic,
        host, cluster);
    assert (feeder.tuples.size() == 2);
    assert (feeder.tuples.get(key1) != null);
    assert (feeder.tuples.get(key2) != null);
    assert (feeder.tuples.get(key1).getTimestamp().getTime() == upperRecieved1);
    assert (feeder.tuples.get(key2).getTimestamp().getTime() == upperRecieved2);
    assert (feeder.tuples.get(key1).getReceived() == 10);
    assert (feeder.tuples.get(key2).getReceived() == 7);

  }

  @Test
  public void testaddTupleDiffTiersSameHost() throws IOException {

    String tier1 = "agent", tier2 = "publisher", host = "localhost", topic = "testTopic", cluster = "testCluster";
    AuditMessage msg1 = new AuditMessage();
    AuditMessage msg2 = new AuditMessage();
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.SECOND, 22);
    cal.set(Calendar.MILLISECOND, 7);
    long msgReceived1 = cal.getTimeInMillis();
    cal.add(Calendar.MINUTE, 1);// second msg for same tier is produced 1 minute
                                // later but having msgs published earlier
    long msgReceived2 = cal.getTimeInMillis();
    System.out.println("Msg 1 generated at " + new Date(msgReceived1));
    msg1.setTimestamp(msgReceived1);
    msg1.setWindowSize(60);
    msg1.setTopic(topic);
    msg1.setTier(tier1);
    msg1.setHostname(host);
    System.out.println("Msg 2 generated at " + new Date(msgReceived2));
    msg2.setTimestamp(msgReceived2);
    msg2.setWindowSize(60);
    msg2.setTopic(topic);
    msg2.setHostname(host);
    msg2.setTier(tier2);
    cal.setTimeInMillis(msgReceived1);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    long received1 = cal.getTimeInMillis();
    System.out.println("MSG 1 has 10 packets of " + new Date(received1));
    msg1.putToReceived(received1, 10);
    System.out.println("MSG 2 has 5 packets of " + new Date(received1));
    msg2.putToReceived(received1, 5);
    cal.add(Calendar.MINUTE, -1);
    long received2 = cal.getTimeInMillis();
    System.out.println("MSG 1 has 7 packets of " + new Date(received2));
    msg1.putToReceived(received2, 7);
    msg1.putToSent(cal.getTimeInMillis(), 5);
    AuditFeederService feeder = new AuditFeederService(cluster, "emtpyRootDir",
        new ClientConfig());
    feeder.addTuples(msg1);
    feeder.addTuples(msg2);
    long upperRecieved1 = received1 + 60 * 1000;
    long upperRecieved2 = received2 + 60 * 1000;
    TupleKey key1 = feeder.new TupleKey(new Date(upperRecieved1), tier1, topic,
        host, cluster);
    TupleKey key2 = feeder.new TupleKey(new Date(upperRecieved2), tier1, topic,
        host, cluster);
    TupleKey key3 = feeder.new TupleKey(new Date(upperRecieved1), tier2, topic,
        host, cluster);
    assert (feeder.tuples.size() == 3);
    assert (feeder.tuples.get(key1) != null);
    assert (feeder.tuples.get(key2) != null);
    assert (feeder.tuples.get(key3) != null);
    assert (feeder.tuples.get(key1).getTimestamp().getTime() == upperRecieved1);
    assert (feeder.tuples.get(key2).getTimestamp().getTime() == upperRecieved2);
    assert (feeder.tuples.get(key3).getTimestamp().getTime() == upperRecieved1);
    assert (feeder.tuples.get(key1).getReceived() == 10);
    assert (feeder.tuples.get(key2).getReceived() == 7);
    assert (feeder.tuples.get(key3).getReceived() == 5);
    assert (feeder.tuples.get(key1).getLatencyCountMap().size() == 1);
    assert (feeder.tuples.get(key2).getLatencyCountMap().size() == 2);
    assert (feeder.tuples.get(key3).getLatencyCountMap().size() == 2);
    assert (feeder.tuples.get(key2).getSent() == 5);

  }

  @Test
  public void testMsgsAlignedAtMin() throws IOException {
    String tier = "agent", host = "localhost", topic = "testTopic", cluster = "testCluster";
    AuditMessage msg = new AuditMessage();
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.SECOND, 20);
    cal.set(Calendar.MILLISECOND, 0);
    long msgReceived = cal.getTimeInMillis();
    System.out.println("Message received at " + new Date(msgReceived));
    msg.setTimestamp(msgReceived);
    msg.setWindowSize(60);
    msg.setTopic(topic);
    msg.setTier(tier);
    msg.setHostname(host);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    long received1 = cal.getTimeInMillis();
    System.out.println("Received 5 packets of " + new Date(received1));
    msg.putToReceived(received1, 5);
    cal.add(Calendar.MINUTE, -1);
    long received2 = cal.getTimeInMillis();
    System.out.println("Received 9 packets of " + new Date(received2));
    msg.putToReceived(received2, 9);
    System.out.println("Sent 5 packets of " + new Date(received2));
    msg.putToSent(received2, 6);
    AuditFeederService feeder = new AuditFeederService(cluster, "emtpyRootDir",
        new ClientConfig());
    AuditMessage[] msgs = feeder.getAuditMessagesAlignedAtMinuteBoundary(msg);
    assert (msgs.length == 2);
    System.out.println(msgs[0].getReceived());
    System.out.println(received1);
    System.out.println(received2);
    System.out.println(new Date(received1));
    System.out.println(new Date(received2));
    assert (msgs[0].getReceived().get(received2) == 6);
    assert (msgs[1].getReceived().get(received2) == 3);
    assert (msgs[1].getReceived().get(received1) == 5);
    assert (msgs[0].getSent().get(received2) == 4);
    assert (msgs[1].getSent().get(received2) == 2);
  }


}
