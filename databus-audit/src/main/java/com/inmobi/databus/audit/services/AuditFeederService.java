package com.inmobi.databus.audit.services;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.databus.audit.AuditService;
import com.inmobi.databus.audit.AuditStats;
import com.inmobi.databus.audit.LatencyColumns;
import com.inmobi.databus.audit.Tuple;
import com.inmobi.databus.audit.util.AuditDBConstants;
import com.inmobi.databus.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.EndOfStreamException;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.databus.DatabusConsumer;
import com.inmobi.messaging.consumer.databus.DatabusConsumerConfig;
import com.inmobi.messaging.consumer.databus.MessagingConsumerConfig;
import com.inmobi.messaging.util.AuditUtil;

/**
 * This class is responsible for reading audit packets,aggregating stats in
 * memory for some time and than performing batch update of the DB
 * 
 * @author rohit.kochar
 * 
 */
public class AuditFeederService extends AuditService {

  class TupleKey {
    public TupleKey(Date timestamp, String tier, String topic, String hostname,
        String cluster) {
      this.timestamp = timestamp;
      this.tier = tier;
      this.topic = topic;
      this.hostname = hostname;
      this.cluster = cluster;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getOuterType().hashCode();
      result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
      result = prime * result + ((hostname == null) ? 0 : hostname.hashCode());
      result = prime * result + ((tier == null) ? 0 : tier.hashCode());
      result = prime * result
          + ((timestamp == null) ? 0 : timestamp.hashCode());
      result = prime * result + ((topic == null) ? 0 : topic.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TupleKey other = (TupleKey) obj;
      if (!getOuterType().equals(other.getOuterType()))
        return false;
      if (cluster == null) {
        if (other.cluster != null)
          return false;
      } else if (!cluster.equals(other.cluster))
        return false;
      if (hostname == null) {
        if (other.hostname != null)
          return false;
      } else if (!hostname.equals(other.hostname))
        return false;
      if (tier == null) {
        if (other.tier != null)
          return false;
      } else if (!tier.equals(other.tier))
        return false;
      if (timestamp == null) {
        if (other.timestamp != null)
          return false;
      } else if (!timestamp.equals(other.timestamp))
        return false;
      if (topic == null) {
        if (other.topic != null)
          return false;
      } else if (!topic.equals(other.topic))
        return false;
      return true;
    }

    Date timestamp;
    String tier, topic, hostname, cluster;

    private AuditFeederService getOuterType() {
      return AuditFeederService.this;
    }

    @Override
    public String toString() {
      return "TupleKey [timestamp=" + timestamp + ", tier=" + tier + ", topic="
          + topic + ", hostname=" + hostname + ", cluster=" + cluster + "]";
    }
  }

  Map<TupleKey, Tuple> tuples = new HashMap<TupleKey, Tuple>();


  private static final Log LOG = LogFactory.getLog(AuditFeederService.class);
  private static final String CONSUMER_CLASSNAME = DatabusConsumer.class
      .getCanonicalName();
  private final String clusterName;
  protected volatile MessageConsumer consumer = null;

  /*
  stopIfMsgNull is added for Unit Tests. If stopIfMsgNull is set to true
  and the msg returned on call of next on consumer is null,
  then iStop is set to true and the feeder stops. Also,
  stops when update fails and consumer is reset.
   */
  protected boolean stopIfMsgNull = false;
  protected volatile boolean isStop = false;

  private int DEFAULT_MSG_PER_BATCH = 5000;
  private int msgsPerBatch;
  private TDeserializer deserializer = new TDeserializer();
  private final ClientConfig config;
  private final static long RETRY_INTERVAL = 60000;
  private final String rootDir;
  private static final String START_TIME_KEY = MessageConsumerFactory.ABSOLUTE_START_TIME;
  private static final String START_FROM_STARTING_KEY = DatabusConsumerConfig.startOfStreamConfig;
  private static final int DEFAULT_TIMEOUT = 30;

  private final Counter messagesProcessed;
  private final Timer timeTakenPerRun, timeTakenDbUpdate;
  private final AuditDBHelper dbHelper;

  /**
   * 
   * @param clusterName
   * @param rootDir
   *          path of _audit stream till /databus
   * @param config
   * @throws IOException
   */
  public AuditFeederService(String clusterName, String rootDir,
      ClientConfig config) throws IOException {
    this.clusterName = clusterName;
    this.config = config;
    this.rootDir = rootDir;
    dbHelper = new AuditDBHelper(config);
    consumer = getConsumer(config);
    msgsPerBatch = config.getInteger(AuditDBConstants.MESSAGES_PER_BATCH,
        DEFAULT_MSG_PER_BATCH);
    LOG.info("Messages per batch " + msgsPerBatch);
    messagesProcessed = AuditStats.metrics.counter(clusterName
        + ".messagesProcessed");
    timeTakenPerRun = AuditStats.metrics
        .timer(clusterName + ".timeTakenPerRun");
    timeTakenDbUpdate = AuditStats.metrics.timer(clusterName
        + ".timeTakenDbUpdate");

  }

  private long getLowerBoundary(long receivedTime, int windowSize) {
    long window = receivedTime - (receivedTime % (windowSize * 1000));
    return window;
  }

  AuditMessage[] getAuditMessagesAlignedAtMinuteBoundary(AuditMessage message) {
    AuditMessage[] messages = new AuditMessage[2];
    int windowSize = message.getWindowSize();
    long receivedTime = message.getTimestamp();
    long lowerBoundary = getLowerBoundary(receivedTime, windowSize);
    long upperBoundary = lowerBoundary + windowSize * 1000;
    messages[0] = new AuditMessage();
    messages[1] = new AuditMessage();
    messages[0].setTimestamp(lowerBoundary);
    messages[1].setTimestamp(upperBoundary);
    for (AuditMessage msg : messages) {
      msg.setTier(message.getTier());
      msg.setHostname(message.getHostname());
      msg.setTopic(message.getTopic());
      msg.setWindowSize(message.getWindowSize());
      msg.setFilenames(message.getFilenames());
      msg.setTags(message.getTags());
    }
    if (message.getReceived() != null) {
      for (Entry<Long, Long> entry : message.getReceived().entrySet()) {
        long upperBoundaryTime = entry.getKey() + windowSize * 1000;
        if (upperBoundaryTime < receivedTime) {
          long received = entry.getValue();
          // dividing the received in proportion of offset of received time from
          // the minute boundary to total window size
          float fractionReceivedInUpperBoundary = ((float) (receivedTime - lowerBoundary))
              / (windowSize * 1000);
          float receivedInUpperBoundary = fractionReceivedInUpperBoundary
              * (float) received;
          long received2 = (long) receivedInUpperBoundary;
          long received1 = received - received2;
          messages[0].putToReceived(entry.getKey(), received1);
          messages[1].putToReceived(entry.getKey(), received2);
        } else {
          // all the received packets belong to same minute interval in which
          // audit is generated
          messages[1].putToReceived(entry.getKey(), entry.getValue());
        }
      }
    }
    if (message.getSent() != null) {
      for (Entry<Long, Long> entry : message.getSent().entrySet()) {
        long upperBoundaryTime = entry.getKey() + windowSize * 1000;
        if (upperBoundaryTime < receivedTime) {
          long sent = entry.getValue();
          // dividing the sent in proportion of offset of received time from
          // the minute boundary to total window size
          float fractionSentInUpperBoundary = ((float) (receivedTime - lowerBoundary))
              / (windowSize * 1000);
          float sentInUpperBoundary = fractionSentInUpperBoundary
              * (float) sent;
          long sent2 = (long) sentInUpperBoundary;
          long sent1 = sent - sent2;
          messages[0].putToSent(entry.getKey(), sent1);
          messages[1].putToSent(entry.getKey(), sent2);
        } else {
          // all the sent packets belong to same minute interval in which
          // audit is generated
          messages[1].putToSent(entry.getKey(), entry.getValue());
        }
      }
    }
    LOG.debug("Audit packet " + message + " was divided into " + messages[0]
        + " and " + messages[1]);
    return messages;
  }

  void addTuples(AuditMessage msg) {
    if (msg == null)
      return;
    int windowSize = msg.getWindowSize();
    for (AuditMessage message : getAuditMessagesAlignedAtMinuteBoundary(msg)) {
      long messageReceivedTime = message.getTimestamp();
      if (message.getReceived() != null) {
        for (long timestamp : message.getReceived().keySet()) {
          long upperBoundaryTime = timestamp + windowSize * 1000;
          long latency = messageReceivedTime - upperBoundaryTime;
          if (latency < 0) {
            LOG.error("Error scenario,check that time is in sync across tiers,audit"
                + "message has time stamp "
                + messageReceivedTime
                + " and source time is "
                + timestamp
                + " for tier "
                + message.getTier());
            continue;
          }
          LatencyColumns latencyColumn = LatencyColumns
              .getLatencyColumn(latency);
          TupleKey key = new TupleKey(new Date(upperBoundaryTime),
              message.getTier(), message.getTopic(), message.getHostname(),
              clusterName);
          Map<LatencyColumns, Long> latencyCountMap = new HashMap<LatencyColumns, Long>();
          Tuple tuple;
          if (tuples.containsKey(key)) {
            tuple = tuples.get(key);
            LOG.debug("Tuple with key " + key + " found in memory with value "
                + tuple);
          } else {
            tuple = new Tuple(message.getHostname(), message.getTier(),
                clusterName, new Date(upperBoundaryTime), message.getTopic());
          }
          if (tuple.getLatencyCountMap() != null) {
            latencyCountMap.putAll(tuple.getLatencyCountMap());
          }
          long prevValue = 0l;
          if (latencyCountMap.get(latencyColumn) != null)
            prevValue = latencyCountMap.get(latencyColumn);
          latencyCountMap.put(latencyColumn, prevValue
              + message.getReceived().get(timestamp));
          tuple.setLatencyCountMap(latencyCountMap);
          tuples.put(key, tuple);
        }
      }
      if (message.getSent() != null) {
        for (long timestamp : message.getSent().keySet()) {
          long upperBoundaryTime = timestamp + windowSize * 1000;
          TupleKey key = new TupleKey(new Date(upperBoundaryTime),
              message.getTier(), message.getTopic(), message.getHostname(),
              clusterName);
          Tuple tuple;
          if (tuples.containsKey(key)) {
            tuple = tuples.get(key);
          } else {
            tuple = new Tuple(message.getHostname(), message.getTier(),
                clusterName, new Date(upperBoundaryTime), message.getTopic());
          }
          long sent = message.getSent().get(timestamp);
          tuple.setSent(tuple.getSent() + sent);
          tuples.put(key, tuple);
        }
      }
    }
  }

  MessageConsumer getConsumer(ClientConfig config) throws IOException {
    config.set(DatabusConsumerConfig.databusRootDirsConfig, rootDir);
    String consumerName = clusterName + "_consumer";
    if (config.getString(MessageConsumerFactory.ABSOLUTE_START_TIME) == null)
      config.set(MessagingConsumerConfig.startOfStreamConfig, "true");
    return MessageConsumerFactory.create(config, CONSUMER_CLASSNAME,
        AuditUtil.AUDIT_STREAM_TOPIC_NAME, consumerName);
  }

  public void stop() {
    isStop = true;
  }

  @Override
  public String getServiceName() {
    return "AuditStatsFeeder_" + clusterName;
  }

  @Override
  public void execute() {
    if (config.getString(START_TIME_KEY) != null) {
    LOG.info("Starting the run of audit feeder for cluster " + clusterName
        + " and start time " + config.getString(START_TIME_KEY));
    } else {
      config.set(START_FROM_STARTING_KEY, "true");
    }
    Message msg;
    AuditMessage auditMsg;
    try {
      while (!isStop) {
        final Timer.Context runContext = timeTakenPerRun.time();
        try {
          int numOfMsgs = 0;
          while (!isStop && consumer == null) {
            // if a checkpoint is already present than from time would be
            // ignored.
            try {
              consumer = getConsumer(config);
            } catch (IOException e) {
              LOG.error("Could not intialize the consumer,would re-try after "
                  + RETRY_INTERVAL + "millis");
              try {
                Thread.sleep(RETRY_INTERVAL);
              } catch (InterruptedException e1) {
                LOG.error("Exception while sleeping", e1);
              }
            }
          }
          while (!isStop && numOfMsgs < msgsPerBatch) {
            try {
              msg = consumer.next(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
              if (msg == null) {// timeout occurred
                if(stopIfMsgNull)
                  isStop = true;
                continue;
              }
              auditMsg = new AuditMessage();
              deserializer.deserialize(auditMsg, msg.getData().array());
              LOG.debug("Packet read is " + auditMsg);
              addTuples(auditMsg);
              numOfMsgs++;
              messagesProcessed.inc();
            } catch (InterruptedException e) {
              LOG.error("Error while reading audit message ", e);
            } catch (TException e) {
              LOG.error("Exception in deserializing audit message");
            } catch (EndOfStreamException e) {
              LOG.info("End of stream reached,breaking the loop");
              isStop = true;
              break;
            }
          }
          if (isStop) {
            LOG.info("Stopped received,not updating in memory contents");
            continue;
          }
          Set<Tuple> tupleSet = new HashSet<Tuple>();
          tupleSet.addAll(tuples.values());
          final Timer.Context dbUpdate = timeTakenDbUpdate.time();
          try {
            if (dbHelper.update(tupleSet)) {
              try {
                consumer.mark();
              } catch (Exception e) {
                LOG.error(
                    "Failure in marking the consumer,Audit Messages  could be re processed",
                    e);
              }
            } else {
              LOG.error("Updation to DB failed,resetting the consumer");
              try {
                consumer.reset();
                if(stopIfMsgNull)
                  isStop = true;
              } catch (Exception e) {
                LOG.error("Exception while reseting the consumer,would re-intialize consumer in next run");
                consumer = null;
              }
            }
          } finally {
            dbUpdate.stop();
          }
          // clearing of the tuples as they have been processed
          tuples.clear();
          if (isStop)
            LOG.info("Stop complete");
        } finally {
          runContext.stop();
        }
      }
    } finally {
      if (consumer != null) {
        LOG.info("closing the consumer for cluster " + clusterName);
        consumer.close();
      }
    }
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getRootDir() {
    return rootDir;
  }
}