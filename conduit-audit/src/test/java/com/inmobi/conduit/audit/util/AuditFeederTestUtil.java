package com.inmobi.conduit.audit.util;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.conduit.audit.Column;
import com.inmobi.conduit.audit.query.AuditDbQuery;
import com.inmobi.conduit.audit.services.AuditFeederService;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.publisher.MockInMemoryPublisher;
import com.inmobi.messaging.util.AuditUtil;
import junit.framework.Assert;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class AuditFeederTestUtil {
  protected AuditMessage oldMsg;
  protected AuditMessage msg1, msg2;
  protected String tier1 = "agent", tier2 = "publisher", host = "localhost",
       topic = "testTopic", cluster = "testCluster", topic1 = "testTopic1";
  protected long msgReceived1, msgReceived2, received1, received2,
      upperRecieved1, upperRecieved2, oldMsgReceived;
  protected MessagePublisher publisher;
  protected int totalData = 10;
  protected Connection connection;
  TSerializer serializer = new TSerializer();

  public void setup() {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.SECOND, 20);
    cal.set(Calendar.MILLISECOND, 0);
    msgReceived1 = cal.getTimeInMillis();//10:20:0
    cal.add(Calendar.MINUTE, 1);
    msgReceived2 = cal.getTimeInMillis();//11:20:0
    cal.setTimeInMillis(msgReceived1);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    received1 = cal.getTimeInMillis(); //10:0:0
    upperRecieved1 = received1 + 60 * 1000;//11:0:0
    cal.add(Calendar.MINUTE, -1);
    received2 = cal.getTimeInMillis();//9:0:0
    upperRecieved2 = received2 + 60 * 1000;//10:0:0
    cal.add(Calendar.DATE, -3);
    oldMsgReceived = cal.getTimeInMillis();

    msg1 = new AuditMessage();
    msg1.setTimestamp(msgReceived1);//10:20:0
    msg1.setWindowSize(60);
    msg1.setTier(tier1);
    msg1.setTopic(topic);
    msg1.setHostname(host);
    msg1.putToReceived(received1, 10);//<10:0:0, 10>
    msg1.putToReceived(received2, 15);//<9:0:0, 15>
    msg1.putToSent(received2, 12);//<9:0:0, 12>

    msg2 = new AuditMessage();
    msg2.setTimestamp(msgReceived2);//11:20:0
    msg2.setWindowSize(60);
    msg2.setTier(tier2);
    msg2.setTopic(topic);
    msg2.setHostname(host);
    msg2.putToReceived(received1, 9);//<10:0:0, 9>
    msg2.putToSent(received1, 6);//<10:0:0, 6>

    oldMsg = new AuditMessage();
    oldMsg.setTimestamp(oldMsgReceived);
    oldMsg.setWindowSize(60);
    oldMsg.setTier(tier1);
    oldMsg.setTopic(topic);
    oldMsg.setHostname(host);
    oldMsg.putToReceived(oldMsgReceived, 10);
    oldMsg.putToSent(oldMsgReceived, 12);
  }

  public void setupPublisher() throws IOException {
    publisher =
        MessagePublisherFactory
            .create("src/test/resources/mock-publisher.properties");
  }

  public void teardown() {
    ((MockInMemoryPublisher) publisher).reset();
  }

  protected void addAuditMessageToPublisher(AuditMessage m) {
    try {
      if (((MockInMemoryPublisher) publisher).source.get(AuditUtil
          .AUDIT_STREAM_TOPIC_NAME) == null) {
        BlockingQueue<Message> list = new LinkedBlockingQueue<Message>();
        list.add(new Message(ByteBuffer.wrap(serializer.serialize(m))));
        ((MockInMemoryPublisher) publisher).source.put(AuditUtil
            .AUDIT_STREAM_TOPIC_NAME, list);
      } else {
        ((MockInMemoryPublisher) publisher).source.get(AuditUtil
            .AUDIT_STREAM_TOPIC_NAME).add(new Message(ByteBuffer.wrap(serializer.serialize(m))));
      }
    } catch (TException e) {
      e.printStackTrace();
    }
  }

  protected void generateData(String topic, int n) {
    String msg = "sample data";
    for (int i = 0; i < n; i++) {
      publisher.publish(topic, new Message(msg.getBytes()));
    }
  }

  public void setupAuditDB() {
    ClientConfig config = ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE);
    String tableName = config.getString(AuditDBConstants.MASTER_TABLE_NAME);
    connection = AuditDBHelper.getConnection(
        config.getString(AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
        config.getString(AuditDBConstants.DB_URL),
        config.getString(AuditDBConstants.DB_USERNAME),
        config.getString(AuditDBConstants.DB_PASSWORD));
    Assert.assertTrue(connection != null);
    String dropTable = "DROP TABLE IF EXISTS " + tableName.toUpperCase() + ";";
    String createTable =
        "CREATE TABLE audit(\n  TIMEINTERVAL bigint,\n  HOSTNAME varchar(50)," +
            "\n  TIER varchar(15),\n  TOPIC varchar(25)," +
            "\n  CLUSTER varchar(50),\n  SENT bigint,\n  C0 bigint," +
            "\n  C1 bigint,\n  C2 bigint,\n  C3 bigint,\n  C4 bigint," +
            "\n  C5 bigint,\n  C6 bigint,\n  C7 bigint,\n  C8 bigint," +
            "\n  C9 bigint,\n  C10 bigint,\n  C15 bigint,\n  C30 bigint," +
            "\n  C60 bigint,\n  C120 bigint,\n  C240 bigint,\n  C600 bigint,\n" +
            "  PRIMARY KEY (TIMEINTERVAL,HOSTNAME,TIER,TOPIC,CLUSTER)\n)";
    try {
      connection.prepareStatement(dropTable).execute();
      connection.prepareStatement(createTable).execute();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  protected ResultSet getAllRowsInAuditDB() {
    String tableName = ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE).getString(
        AuditDBConstants.MASTER_TABLE_NAME);
    String selectAllRows = "SELECT * FROM "+tableName+" ORDER BY "+ Column
        .TOPIC.toString();
    ResultSet rs = null;
    try {
      rs = connection.prepareStatement(selectAllRows).executeQuery();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return rs;
  }

  protected int getNumberOfRowsInAuditDB() {
    String tableName = ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE).getString(
        AuditDBConstants.MASTER_TABLE_NAME);
    String countStmt = "SELECT COUNT(*) AS NUMTUPLES FROM "+tableName;
    int n = -1;
    try {
      ResultSet rs = connection.prepareStatement(countStmt).executeQuery();
      assert (rs.next());
      n = rs.getInt("NUMTUPLES");
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return n;
  }

  protected void addConstraintToAuditDB() {
    String tableName = ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE).getString(
        AuditDBConstants.MASTER_TABLE_NAME);
    String addConstraint = "ALTER TABLE "+tableName+" ADD CONSTRAINT " +
        "topicConstraint CHECK("+Column.TOPIC.toString()+"='"+topic+"');";
    try {
      connection.prepareStatement(addConstraint).execute();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  protected void dropConstraintOfAuditDB() {
    String tableName = ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE).getString(
        AuditDBConstants.MASTER_TABLE_NAME);
    String addConstraint = "ALTER TABLE "+tableName+" DROP CONSTRAINT " +
        "topicConstraint;";
    try {
      connection.prepareStatement(addConstraint).execute();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void shutDownAuditDB() {
    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void shutDown() {
    //To change body of created methods use File | Settings | File Templates.
  }
}
