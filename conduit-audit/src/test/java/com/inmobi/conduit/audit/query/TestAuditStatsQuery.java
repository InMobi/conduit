package com.inmobi.conduit.audit.query;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.conduit.audit.Column;
import com.inmobi.conduit.audit.Filter;
import com.inmobi.conduit.audit.GroupBy;
import com.inmobi.conduit.audit.GroupBy.Group;
import com.inmobi.conduit.audit.Tier;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.EndOfStreamException;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MockInMemoryConsumer;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.publisher.MockInMemoryPublisher;
import com.inmobi.messaging.util.AuditUtil;

public class TestAuditStatsQuery {
  private MessagePublisher publisher;
  private String startTime, endTime;
  private Date startDate, endDate;
  private SimpleDateFormat formatter = new SimpleDateFormat(
      AuditUtil.DATE_FORMAT);

  private int totalData = 10;// put an even no.
  String topic = "topic1";
  String topic1 = "topic2";
  String topic2 = "topic3";
  String topic3 = "topic4";
  String topic4 = "topic5";
  String topic5 = "topic6";

  @BeforeMethod
  public void setup() throws IOException {
    publisher =
        MessagePublisherFactory
            .create("src/test/resources/mock-publisher.properties");

  }

  @AfterMethod
  public void teardown() {
    ((MockInMemoryPublisher) publisher).reset();
  }

  private void generateData(MessagePublisher publisher, String topic,
                            String topic1) {
    String msg = "sample data";
    startDate = new Date();
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(startDate);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    startTime = formatter.format(calendar.getTime());
    for (int i = 0; i < totalData / 2; i++) {
      publisher.publish(topic, new Message(msg.getBytes()));
    }
    for (int i = 0; i < totalData / 2; i++) {
      publisher.publish(topic1, new Message(msg.getBytes()));
    }
    endDate = new Date();
    endTime = formatter.format(endDate);
  }

  private void generateData(String topic, String topic1) {
    generateData(publisher, topic, topic1);
  }

  @Test
  public void testAuditQuery() throws IOException, InterruptedException,
      TException, ParseException, EndOfStreamException {
    assert (publisher instanceof com.inmobi.messaging.publisher.MockInMemoryPublisher);
    generateData(topic, topic1);
    publisher.close();
    AuditStatsQuery query =
        new AuditStatsQuery("mock", endTime, startTime, "topic=" + topic1,
            null, null, "1", null);
    query.parseAndSetArguments();
    query.timeout = 10;
    MessageConsumer consumer = query.getConsumer(startDate, endDate, "mock");
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    query.aggregateStats(consumer);
    Collection<Long> sent = query.getReceived().values();
    assert (sent.iterator().hasNext());
    Long sentPublisher = sent.iterator().next();
    assert (sentPublisher == totalData / 2);

  }

  @Test
  public void testAuditQueryGroupFilter() throws IOException,
      InterruptedException, TException, ParseException, EndOfStreamException {
    assert (publisher instanceof com.inmobi.messaging.publisher.MockInMemoryPublisher);
    generateData(topic, topic1);
    publisher.close();
    AuditStatsQuery query =
        new AuditStatsQuery("mock", endTime, startTime, null, "topic,tier",
            null, "1", null);
    query.parseAndSetArguments();
    query.timeout = 10;
    MessageConsumer consumer = query.getConsumer(startDate, endDate, "mock");
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    query.aggregateStats(consumer);

    Map<Column, String> map = new HashMap<Column, String>();
    map.put(Column.TIER, "publisher");
    map.put(Column.TOPIC, topic);
    Group grp = query.groupBy.getGroup(map);
    assert (query.getReceived().size() == 2);
    assert (query.getReceived().get(grp) == totalData / 2);
    map = new HashMap<Column, String>();
    map.put(Column.TIER, "publisher");
    map.put(Column.TOPIC, topic1);
    grp = query.groupBy.getGroup(map);
    assert (query.getReceived().get(grp) == totalData / 2);
  }

  @Test
  public void testAuditQueryWhereEndTimeIsLessThanFromTime()
      throws IOException, InterruptedException, TException, ParseException, EndOfStreamException {
    assert (publisher instanceof com.inmobi.messaging.publisher.MockInMemoryPublisher);
    generateData(topic, topic1);
    publisher.close();
    AuditStatsQuery query =
        new AuditStatsQuery("mock", endTime, startTime, "topic=" + topic1,
            null, null, "1", null);
    query.parseAndSetArguments();

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(startDate);
    calendar.add(Calendar.MINUTE, -1);
    query.toTime = calendar.getTime();
    query.timeout = 10;
    MessageConsumer consumer = query.getConsumer(startDate, endDate, "mock");
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    query.aggregateStats(consumer);
    Collection<Long> sent = query.getReceived().values();
    assert (!sent.iterator().hasNext());

  }

  @Test
  public void testAuditQueryMaxMessages() throws IOException,
      InterruptedException, TException, ParseException, EndOfStreamException {
    assert (publisher instanceof com.inmobi.messaging.publisher.MockInMemoryPublisher);
    generateData(topic4, topic5);
    // setting very large timeout so that query cuttoff should happen as per max
    // messages
    AuditStatsQuery query = new AuditStatsQuery("mock", endTime, startTime,
        "topic=" + topic4, null, null, "600", null, Tier.PUBLISHER,
        totalData / 2);
    query.parseAndSetArguments();
    publisher.close();
    query.filter = new Filter("topic=" + topic4);
    query.groupBy = new GroupBy(null);
    MessageConsumer consumer = query.getConsumer(startDate, endDate, "mock");
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    query.aggregateStats(consumer);
    Collection<Long> sent = query.getReceived().values();
    assert (sent.iterator().hasNext());
    Long sentPublisher = sent.iterator().next();
    assert (sentPublisher == totalData / 2);

  }

  @Test
  public void testGroupCompare() {
    GroupBy grpBy = new GroupBy("tier,topic");
    Map<Column, String> values1 = new HashMap<Column, String>();
    Map<Column, String> values2 = new HashMap<Column, String>();
    values1.put(Column.HOSTNAME, "host1");
    values1.put(Column.TIER, "agent");
    values1.put(Column.TOPIC, "topic1");

    values2.put(Column.HOSTNAME, "host2");
    values2.put(Column.TIER, "publisher");
    values2.put(Column.TOPIC, "topic2");
    Group grp1 = grpBy.getGroup(values1);
    Group grp2 = grpBy.getGroup(values2);
    int result = grp1.compareTo(grp2);
    assert (result == 1);
  }

 }