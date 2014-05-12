package com.inmobi.conduit.audit.validator;

import com.inmobi.conduit.audit.Tier;
import com.inmobi.conduit.audit.Tuple;
import com.inmobi.conduit.audit.query.AuditDbQuery;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.util.AuditUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class AuditValidator {
  private static final int minArgs = 3;
  private static final int DEFAULT_NUM_MSGS_PER_TIME_INTERVAL = 1000;
  private static final int DEFAULT_TIME_INTERVAL = 1;
  private static final int MESSAGE_SIZE_LENGTH = 2000;
  private static final int DEFAULT_PUBLISHER_WAIT_IN_MINS = 10;
  private static final int DEFAULT_AGENT_WAIT_IN_MINS = 10;
  private static final int DEFAULT_COLLECTOR_WAIT_IN_MINS = 12;
  private static final int DEFAULT_HDFS_WAIT_IN_MINS = 12;
  private static final int DEFAULT_LOCAL_WAIT_IN_MINS = 15;
  private static final String DEFAULT_COLLECTOR_CONF_FILE =
      "messaging-consumer-collector-conf.properties";
  private static final String DEFAULT_LOCAL_CONF_FILE =
      "messaging-consumer-local-conf.properties";
  private static final String DELIMITER = "/t";
  private static final String VALIDATOR_GROUPBY_STRING = "TIER";
  private static final Log LOG = LogFactory.getLog(AuditValidator.class);
  private static final SimpleDateFormat auditDateFormat = new
      SimpleDateFormat(AuditUtil.DATE_FORMAT);

  private String topic;
  private long numOfMsgs;
  private int timeIntervalInMins;
  private int numPublishers;
  private Date startDate = null;
  private String collectorConfFile, localConfFile;
  private PublisherWorker publisherWorker = null;
  private Map<Tier, ConsumerWorker> consumerWorkerMap = null;
  private Map<Tier, AuditWorker> auditWorkerMap = null;
  private Map<Tier, Integer> tierWaitMap = null;
  private Map<Tier, Map<Long, Long>> tierTimeCountMap = null;
  private final boolean runPublisher, runConsumer;

  public AuditValidator(String topic, int numOfMsgs, int timeInterval,
                        boolean runPublisher, boolean runConsumer,
                        int numPublishers, String collectorConfFile,
                        String localConfFile, String startTime) {
    if (collectorConfFile == null || collectorConfFile.length() == 0) {
      collectorConfFile = DEFAULT_COLLECTOR_CONF_FILE;
    }
    if (localConfFile == null || localConfFile.length() == 0) {
      localConfFile = DEFAULT_LOCAL_CONF_FILE;
    }
    this.topic = topic;
    this.numOfMsgs = numOfMsgs;
    this.timeIntervalInMins = timeInterval;
    this.runPublisher = runPublisher;
    this.runConsumer = runConsumer;
    this.numPublishers = numPublishers;
    this.collectorConfFile = collectorConfFile;
    this.localConfFile = localConfFile;
    if (startTime != null) {
      try {
        this.startDate = auditDateFormat.parse(startTime);
      } catch (ParseException e) {
        LOG.error("ParseException while parsing startTime passed from cli " +
            startTime, e);
      }
    }
    if (this.startDate == null) {
      this.startDate = new Date();
    }
    consumerWorkerMap = new HashMap<Tier, ConsumerWorker>();
    auditWorkerMap = new HashMap<Tier, AuditWorker>();
    tierTimeCountMap = new HashMap<Tier, Map<Long, Long>>();
    initializeTierWaitMap();
  }

  private void initializeTierWaitMap() {
    if (tierWaitMap == null) {
      tierWaitMap = new HashMap<Tier, Integer>();
    }
    tierWaitMap.put(Tier.PUBLISHER, DEFAULT_PUBLISHER_WAIT_IN_MINS);
    tierWaitMap.put(Tier.AGENT, DEFAULT_AGENT_WAIT_IN_MINS);
    tierWaitMap.put(Tier.COLLECTOR, DEFAULT_COLLECTOR_WAIT_IN_MINS);
    tierWaitMap.put(Tier.HDFS, DEFAULT_HDFS_WAIT_IN_MINS);
    tierWaitMap.put(Tier.LOCAL, DEFAULT_LOCAL_WAIT_IN_MINS);
  }

  public static void main(String[] args) {
    if (args.length < minArgs) {
      System.out.println("Insufficient number of arguments");
      printUsage();
      System.exit(-1);
    }
    LOG.info("Number of arguments:"+ args.length);
    String topic = null;
    int numOfMsgs = 0;
    int timeInterval = 0;
    int numPublishers = 1;
    boolean runPublisher = false, runConsumer = false;
    String localConfFile = null, collectorConfFile = null, startTime = null;
    for (int i = 0; i < args.length;) {
      LOG.info("current args:" + args[i]);
      if (args[i].equals("--publish")) {
        runPublisher = true;
        LOG.info("Will run publisher worker");
        i++;
      } else if (args[i].equals("--consume")) {
        runConsumer = true;
        LOG.info("Will run consumer worker");
        i++;
      } else if (args[i].equals("--topic")) {
        topic = args[i+1];
        LOG.info(" for topic: " + topic);
        i = i + 2;
      } else if (args[i].equals("--local")) {
        localConfFile = args[i+1];
        LOG.info(" with local consumer conf:" + localConfFile);
        i = i + 2;
      } else if (args[i].equals("--collector")) {
        collectorConfFile = args[i+1];
        LOG.info(" with collector consumer conf:" + collectorConfFile);
        i = i + 2;
      } else if (args[i].equals("--numPublishers")) {
        numPublishers = Integer.parseInt(args[i+1]);
        LOG.info(" with num of publishers:" + numPublishers);
        i = i + 2;
      } else if (args[i].equals("--startTime")) {
        startTime = args[i+1];
        LOG.info(" with start time as:" + startTime);
        i = i + 2;
      } else if (args[i].equals("-n") || args[i].equals("--numMessages")) {
        try {
          numOfMsgs = Integer.parseInt(args[i+1]);
          LOG.info(" with num of messages:" + numOfMsgs);
          i = i + 2;
        } catch (NumberFormatException e) {
          System.out.println("Encountered NumberFormatException while " +
              "parsing number of messages:"+args[i+1]);
          printUsage();
          System.exit(-1);
        }
      } else if (args[i].equals("-t") || args[i].equals("--interval")) {
        try {
          timeInterval = Integer.parseInt(args[i+1]);
          LOG.info(" with time interval:" + timeInterval);
          i = i + 2;
        } catch (NumberFormatException e) {
          System.out.println("Encountered NumberFormatException while " +
              "parsing time interval:"+args[i+1]);
          printUsage();
          System.exit(-1);
        }
      } else {
        System.out.println("Unexpected argument passed");
        printUsage();
        System.exit(-1);
      }
    }
    if (numOfMsgs == 0) {
      numOfMsgs = DEFAULT_NUM_MSGS_PER_TIME_INTERVAL;
    }
    if (timeInterval == 0) {
      timeInterval = DEFAULT_TIME_INTERVAL;
    }

    final AuditValidator validator = new AuditValidator(topic, numOfMsgs,
        timeInterval, runPublisher, runConsumer, numPublishers,
        collectorConfFile, localConfFile, startTime);
    validator.execute();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        validator.stop();
        LOG.info("Finishing the shutdown hook");
      }
    });
  }

  public synchronized void stop() {
    if (runPublisher && publisherWorker != null) {
      publisherWorker.interrupt();
      publisherWorker.isStop = true;
    }

    if (runConsumer) {
      for (ConsumerWorker worker : consumerWorkerMap.values()) {
        worker.interrupt();
        worker.isStop = true;
      }

      for (AuditWorker worker : auditWorkerMap.values()) {
        worker.interrupt();
        worker.isStop = true;
      }
    }
  }

  public synchronized void execute() {
    if (runConsumer) {
      startConsumers(Tier.HDFS, collectorConfFile);
      startConsumers(Tier.LOCAL, localConfFile);
    }

    if (runPublisher) {
      publisherWorker = startPublishingMessages();
    }

    try {
      Thread.sleep(60000);
    } catch (InterruptedException e) {
      LOG.error("Validator thread interrupted while sleeping before starting " +
          "audit validation threads", e);
    }

    if (runConsumer) {
      startAuditValidation(Tier.PUBLISHER);
      startAuditValidation(Tier.HDFS);
      startAuditValidation(Tier.LOCAL);
    }
  }

  public static void printUsage() {
    System.out.println("Usage:");
    System.out.println("[--publish ]");
    System.out.println("[--consume [--numPublishers <num of publishers> " +
        "--collector <file name of conf for collector consumer> --local <file" +
        " name of conf for local consumer> --startTime <in dd-MM-yyyy-HH:mm>]]");
    System.out.println("--topic <audit validator stream name> ");
    System.out.println("[-n|--numMessages <number of messages published per " +
        "time interval>] ");
    System.out.println("[-t|--interval <Time interval in minutes to publish " +
        "messages to test stream>] ");
    System.out.println("-c|--conf <path to conf dir>");
  }

  private PublisherWorker startPublishingMessages() {
    PublisherWorker publisherWorker = new PublisherWorker();
    publisherWorker.start();
    return publisherWorker;
  }

  private void startAuditValidation(Tier tier) {
    AuditWorker auditWorker;
    if (tier.equals(Tier.PUBLISHER)) {
      auditWorker = new AuditWorker(tier, Tier.AGENT);
    } else if (tier.equals(Tier.HDFS)) {
      auditWorker = new AuditWorker(tier, Tier.COLLECTOR);
    } else {
      auditWorker = new AuditWorker(tier);
    }
    auditWorker.start();
    auditWorkerMap.put(tier, auditWorker);
  }

  private void startConsumers(Tier tier, String confFile) {
    ConsumerWorker consumerWorker = new ConsumerWorker(tier, numPublishers,
        confFile);
    consumerWorker.start();
    consumerWorkerMap.put(tier, consumerWorker);
  }

  private Long getMinute(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.MILLISECOND, 0);
    calendar.set(Calendar.SECOND, 0);
    return calendar.getTimeInMillis();
  }

  private Date getNextRunStartTime() {
    return getNextRunStartTime(new Date());
  }

  private Date getNextRunStartTime(Date currentTime) {
    long toAddMilliSeconds = getTimeForWorkerSleep(currentTime);
    return new Date(currentTime.getTime() + toAddMilliSeconds);
  }

  private long getTimeForWorkerSleep() {
    return getTimeForWorkerSleep(new Date());
  }

  private long getTimeForWorkerSleep(Date currentDate) {
    long currentTime = currentDate.getTime();
    return (timeIntervalInMins * 60000) - (currentTime % (timeIntervalInMins
        * 60000));
  }

  private class ConsumerWorker extends Thread {
    private Tier tier;
    private volatile boolean isStop = false;
    private ClientConfig consumerConfig;
    private MessageConsumer consumer;
    private int numPublishers;
    Map<Long, Long> nextElementToPurgeMap;
    Map<Long, Map<Long, Integer>> timeSeqProducerCountMap;

    public ConsumerWorker(Tier tier, int numPublishers, String confFile) {
      this.tier = tier;
      this.numPublishers = numPublishers;
      nextElementToPurgeMap = new HashMap<Long, Long>();
      timeSeqProducerCountMap = new HashMap<Long, Map<Long, Integer>>();
      consumerConfig =
          ClientConfig.loadFromClasspath(confFile);
    }

    String getMessage(Message msg)
        throws IOException {
      byte[] data = msg.getData().array();
      return new String(data);
    }

    private void purgeCounts(Long date) {
      Map<Long, Integer> seqCountMap = timeSeqProducerCountMap.get(date);
      Long nextElementToPurge = nextElementToPurgeMap.get(date);
      Set<Map.Entry<Long, Integer>> entrySet = seqCountMap.entrySet();
      Iterator<Map.Entry<Long, Integer>> iter = entrySet.iterator();
      while (iter.hasNext()) {
        Map.Entry<Long, Integer> entry = iter.next();
        long msgIndex = entry.getKey();
        int pcount = entry.getValue();
        if (seqCountMap.size() > 1) {
          if (msgIndex == nextElementToPurge) {
            if (pcount >= numPublishers) {
              iter.remove();
              nextElementToPurge++;
              continue;
            }
          }
        }
        break;
      }
      if (nextElementToPurge > numOfMsgs) {
        timeSeqProducerCountMap.remove(date);
        nextElementToPurgeMap.remove(date);
      } else {
        nextElementToPurgeMap.put(date, nextElementToPurge);
      }
    }

    @Override
    public void run() {
      LOG.info("Starting consumer and setting name of consumer thread");
      Thread.currentThread().setName(tier.toString() + "ConsumerWorker");
      try {
        consumer = MessageConsumerFactory.create(consumerConfig, startDate);
      } catch (IOException e) {
        LOG.error("Encountered IOException when initializing consumer", e);
      }

      while (!isStop) {
        Message msg;
        try {
          msg = consumer.next();
          String s = getMessage(msg);
          String[] splitStrings = s.split(DELIMITER);
          assert splitStrings.length == 4;
          Long date = Long.parseLong(splitStrings[0]);
          Map<Long, Long> timeCountMap = tierTimeCountMap.get(tier);
          if (timeCountMap == null) {
            timeCountMap = new HashMap<Long, Long>();
          }
          Long currentCount = timeCountMap.get(date);
          if (currentCount == null) {
            timeCountMap.put(date, 1l);
          } else {
            timeCountMap.put(date, currentCount + 1);
          }
          tierTimeCountMap.put(tier, timeCountMap);
          Long seq = Long.parseLong(splitStrings[1]);
          Map<Long, Integer> seqProducerMap = timeSeqProducerCountMap.get(date);
          if (seqProducerMap == null) {
            seqProducerMap = new TreeMap<Long, Integer>();
          }
          Integer pcount = seqProducerMap.get(seq);
          Long nextElementToPurge = nextElementToPurgeMap.get(date);
          if (nextElementToPurge == null) {
            nextElementToPurge = 1l;
            nextElementToPurgeMap.put(date, 1l);
          }
          if (seq < nextElementToPurge) {
            LOG.error("Duplicate message found at tier[" + tier + "] " +
                "produced at time[" + date + "] by host[" + splitStrings[2] +
                "]");
          } else {
            if (pcount == null) {
              seqProducerMap.put(seq, new Integer(1));
            } else {
              pcount++;
              if (pcount > numPublishers) {
                LOG.error("Duplicate message found at tier[" + tier + "] " +
                    "produced at time[" + date + "], " +
                    "number of publishers publishing message of seqeunce " +
                    "number " + seq + " is greater than expected, " +
                    "found [" + pcount + "] expected [" + numPublishers + "]");
              }
              seqProducerMap.put(seq, pcount);
            }
          }
          timeSeqProducerCountMap.put(date, seqProducerMap);
          purgeCounts(date);
        } catch (Exception e) {
          LOG.error("Exception while getting next message", e);
        }
      }

      consumer.close();
    }
  }

  private class AuditWorker extends Thread {
    private Tier primaryTier, secondaryTier;
    private volatile boolean isStop = false;

    public AuditWorker(Tier tier) {
      this(tier, null);
    }

    public AuditWorker(Tier primaryTier, Tier secondaryTier) {
      this.primaryTier = primaryTier;
      this.secondaryTier = secondaryTier;
    }

    @Override
    public void run() {
      String threadName;
      Date runStartTime = new Date(startDate.getTime());
      if (secondaryTier != null) {
        threadName = primaryTier.toString() + secondaryTier.toString() +
            "AuditWorker";
      } else {
        threadName = primaryTier + "AuditWorker";
      }
      Thread.currentThread().setName(threadName);

      String filterString;
      if (secondaryTier != null) {
        filterString = "TOPIC=" + topic + ",TIER=" + primaryTier.toString()
            .toLowerCase() + "|" + secondaryTier.toString().toLowerCase();
      } else {
        filterString = "TOPIC=" + topic + ",TIER=" + primaryTier.toString()
            .toUpperCase();
      }

      while (!isStop) {
        Date runEndTime = getNextRunStartTime(runStartTime);
        AuditDbQuery auditQuery = new AuditDbQuery(auditDateFormat.format
            (runEndTime), auditDateFormat.format(runStartTime),
            filterString, VALIDATOR_GROUPBY_STRING, null);
        if (!isStop) {
          try {
            auditQuery.execute();
            Long expectedNumMsgs;
            if (primaryTier.equals(Tier.PUBLISHER)) {
              expectedNumMsgs = numOfMsgs;
            } else {
              expectedNumMsgs = tierTimeCountMap.get(primaryTier).get
                  (runStartTime.getTime());
              if (expectedNumMsgs == null) {
                expectedNumMsgs = 0l;
              }
            }
            if (secondaryTier != null) {
              assert auditQuery.getTupleSet().size() == 2;
            } else {
              assert auditQuery.getTupleSet().size() == 1;
            }
            Tuple primaryTuple = null, secondaryTuple = null;
            for (Tuple tuple : auditQuery.getTupleSet()) {
              if (tuple.getTier().equalsIgnoreCase(primaryTier.toString())) {
                primaryTuple = tuple;
              } else if (secondaryTier != null && tuple.getTier()
                  .equalsIgnoreCase(secondaryTier.toString())) {
                secondaryTuple = tuple;
              }
            }
            if (primaryTuple == null) {
              LOG.error("No tuple found corresponding to tier:" + primaryTier
                  + " when querying between[" + auditDateFormat.format
                  (runStartTime) + ", " + auditDateFormat.format(runEndTime)
                  + "]");
            }
            if (primaryTuple != null && primaryTuple.getReceived() !=
                expectedNumMsgs){
              LOG.error("Audit showing incorrect number of messages at tier" +
                  primaryTier + " expected [" + expectedNumMsgs + "] actual ["
                  + primaryTuple.getReceived() + "] for time interval[" +
                  auditDateFormat.format(runStartTime) + ", " +
                  "" + auditDateFormat.format(runEndTime) + "]");
            }
            if (secondaryTier != null && secondaryTuple == null) {
              LOG.error("No tuple found corresponding to tier: " +
                  secondaryTier + " when querying between [" + auditDateFormat
                  .format(runStartTime) + ", " + auditDateFormat.format(runEndTime)
                  + "]");
            }
            if (secondaryTier != null && secondaryTuple != null &&
                secondaryTuple.getReceived() != expectedNumMsgs) {
              LOG.error("Audit showing incorrect number of messages at tier " +
                  secondaryTier + " expected [" + expectedNumMsgs + "] actual" +
                  " [" + secondaryTuple.getReceived() + "] for time " +
                  "interval[" + auditDateFormat.format(runStartTime) + ", " +
                  "" + auditDateFormat.format(runEndTime) + "]");
              if (secondaryTuple.getReceived() != primaryTuple.getReceived()) {
                LOG.error("Audit also showing mismatch in number of messages " +
                    "recieved between " + primaryTier + " [" + primaryTuple
                    .getReceived() + "] and " + secondaryTier + " [" +
                    secondaryTuple.getReceived() + "]");
              }
            }
            if (!primaryTier.equals(Tier.PUBLISHER)) {
              tierTimeCountMap.get(primaryTier).remove(runStartTime);
            }
          } catch (Exception e) {
            LOG.error("Exception when executing AuditDBQuery" + auditQuery
                .toString(), e);
          }
        }
        runStartTime = runEndTime;
        if (!isStop) {
          long sleepMilliSeconds = getTimeForWorkerSleep();
          try {
            LOG.debug("Sleeping till next run for " + sleepMilliSeconds +
                "ms");
            sleep(sleepMilliSeconds);
          } catch (InterruptedException e) {
            LOG.error("Interrupted publisher worker thread", e);
          }
        }
      }
    }
  }

  private class PublisherWorker extends Thread {

    private AbstractMessagePublisher publisher = null;
    private volatile boolean isStop = false;
    private byte[] randomMsg = null;
    private long numMsgsFiveSecs;
    private long numMsgsLeft;

    @Override
    public void run() {
      Thread.currentThread().setName("PublisherWorker");
      numMsgsFiveSecs = (int) numOfMsgs / ((timeIntervalInMins * 12) - 1);
      numMsgsLeft = numOfMsgs - (numMsgsFiveSecs * ((timeIntervalInMins * 12)
          - 1));
      LOG.info("Number of messages to be published per time interval: " +
          numOfMsgs);
      LOG.info("Number of messages to be published per 5 sec:" + numMsgsFiveSecs);

      while (!isStop) {
        long sleepMilliSeconds = getTimeForWorkerSleep();
        try {
          LOG.debug("Sleeping till next run for " + sleepMilliSeconds + "ms");
          sleep(sleepMilliSeconds);
        } catch (InterruptedException e) {
          LOG.error("Interrupted publisher worker thread", e);
        }

        Date runStartTime = new Date();
        Date runBoundaryTime = getBoundaryTimeForCurrentTime
            (getNextRunStartTime());
        if (publisher == null) {
          createPublisher();
        }
        if (randomMsg == null) {
          randomMsg = getMessageBytes(MESSAGE_SIZE_LENGTH);
        }

        if (!isStop) {
          // sleep for 3 seconds to ensure that publishing of messages starts
          // after 3 sec of start of current time interval
          try {
            sleep(3000);
          } catch (InterruptedException e) {
            LOG.error("Interrupted publisher worker thread", e);
          }
        }

        if (!isStop) {
          long index = 1;
          long i = 1;
          long run = 1;
          // Publish message only if the number of messages published till now
          // does not exceed the maximum number of messages and if the time at
          // the time of publishing is less than (time interval boundary - 1
          // sec) (keeping 1 sec time boundary to allow for publishing the last
          // message) and if validator is not stopped
          while (index <= numOfMsgs && !(new Date().after(runBoundaryTime))
              && !isStop) {
            boolean publish = false;
            if ((run == 1 && i <= numMsgsFiveSecs + numMsgsLeft) || (run != 1
                && i <= numMsgsFiveSecs)) {
              publish = true;
            }
            if (publish) {
              publisher.publish(topic, constructMessage(runStartTime, index,
                  randomMsg));
              index++;
              i++;
              if ((run == 1 && i > numMsgsFiveSecs + numMsgsLeft) || (run !=
                  1 && i > numMsgsFiveSecs)) {
                LOG.debug("Sleeping to maintain constant rate");
                try {
                  sleep(5000);
                } catch (InterruptedException e) {
                  LOG.error("Sleep interrupted while publishing messages at " +
                      "constant rate", e);
                }
                i = 1;
                run++;
              }
            }
          }
          Date runEndTime = new Date();
          LOG.info("Published [" + (index - 1) + "] messages between [" +
              runStartTime +  ", " + runEndTime + "]");
        }
      }
    }

    private Date getBoundaryTimeForCurrentTime(Date runStartTime) {
      return new Date(runStartTime.getTime() - 2000);
    }

    //Defining method again as it is not public in StreamingBenchmark class
    private byte[] getMessageBytes(int messageSizeLength) {
      byte[] msg = new byte[messageSizeLength];
      for (int i = 0; i < messageSizeLength; i++) {
        msg[i] = 'A';
      }
      return msg;
    }

    //Defining method again as it is not public in StreamingBenchmark class
    Message constructMessage(Date date, long msgIndex,
                             byte[] randomBytes) {
      String s = null;
      try {
        s = Long.toString(getMinute(date)) + DELIMITER + msgIndex +
            DELIMITER + InetAddress.getLocalHost().getHostName() + DELIMITER;
      } catch (UnknownHostException e) {
        LOG.error("Unable to get hostname of publisher", e);
      }
      byte[] msgBytes = new byte[s.length() + randomBytes.length];
      System.arraycopy(s.getBytes(), 0, msgBytes, 0, s.length());
      System.arraycopy(randomBytes, 0, msgBytes, s.length(), randomBytes.length);
      return new Message(ByteBuffer.wrap(msgBytes));
    }

    private void createPublisher() {
      try {
        publisher = (AbstractMessagePublisher) MessagePublisherFactory.create();
      } catch (IOException e) {
        LOG.error("IOException when creating publisher", e);
      }
    }
  }
}
