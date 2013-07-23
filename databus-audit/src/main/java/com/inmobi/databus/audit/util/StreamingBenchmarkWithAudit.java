package com.inmobi.databus.audit.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;

import com.inmobi.databus.audit.query.AuditDbQuery;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.examples.StreamingBenchmark;
import com.inmobi.messaging.util.AuditUtil;

public class StreamingBenchmarkWithAudit {

  static final String DELIMITER = "/t";
  static final SimpleDateFormat LogDateFormat = new SimpleDateFormat(
      "yyyy:MM:dd hh:mm:ss");

  static final int WRONG_USAGE_CODE = -1;
  static final int FAILED_CODE = 1;

  static int printUsage() {
    System.out.println("Usage: StreamingBenchmarkWithAudit  "
        + " [-producer <topic-name> <no-of-msgs> <no-of-msgs-per-sec>"
        + " [<timeoutSeconds> <msg-size> <no-of-threads>]]"
        + " [-consumer <no-of-producers> <no-of-msgs>"
        + " [<timeoutSeconds> <msg-size> <hadoopconsumerflag> "
        + " <auditTimeoutInSecs> <timezone>]]");
    return WRONG_USAGE_CODE;
  }

  public static void main(String[] args) throws Exception {
    int exitcode = run(args);
    System.exit(exitcode);
  }

  static int numProducerArgs = 5;
  static int numProducerRequiredArgs = 3;
  static int numConsumerArgs = 5;
  static int numConsumerRequiredArgs = 2;
  static int minArgs = 3;

  public static int run(String[] args) throws Exception {
    if (args.length < minArgs) {
      return printUsage();
    }
    int consumerTimeout = 0;
    long maxSent = -1;
    int numProducers = 1;
    String auditStartTime = null;
    String auditEndTime = null;
    String auditTopic = null;
    int auditTimeout = 30;
    boolean runConsumer = false;
    SimpleDateFormat formatter = new SimpleDateFormat(AuditUtil.DATE_FORMAT);

    if (args.length >= minArgs) {
      int consumerOptionIndex = -1;
      if (args[0].equals("-producer")) {
        if (args.length < (numProducerRequiredArgs + 1)) {
          return printUsage();
        }
        if (args.length > 4 && !args[4].equals("-consumer")) {
          if (args.length > 5 && !args[5].equals("-consumer")) {
            if (args.length > 6 && !args[6].equals("-consumer")) {

              consumerOptionIndex = 7;
            } else {
              consumerOptionIndex = 6;
            }
          } else {
            consumerOptionIndex = 5;
          }
        } else {
          consumerOptionIndex = 4;
        }
      } else {
        consumerOptionIndex = 0;
      }

      if (args.length > consumerOptionIndex) {
        if (args[consumerOptionIndex].equals("-consumer")) {
          numProducers = Integer.parseInt(args[consumerOptionIndex + 1]);
          maxSent = Long.parseLong(args[consumerOptionIndex + 2]);
          if (args.length > consumerOptionIndex + 3) {
            consumerTimeout = Integer.parseInt(args[consumerOptionIndex + 3]);
            System.out.println("consumerTimeout :" + consumerTimeout
                + " seconds");
          }
          if (args.length > consumerOptionIndex + 6) {
            auditTimeout = Integer.parseInt(args[consumerOptionIndex + 6]);
          }
          runConsumer = true;
        }
      }
    } else {
      return printUsage();
    }
    StreamingBenchmark benchmark = new StreamingBenchmark();
    benchmark.run(args);
    if (runConsumer) {
      ClientConfig config = ClientConfig
          .loadFromClasspath(MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
      // set consumer start time as auditStartTime
      auditStartTime = formatter.format(benchmark.getConsumerStartTime());
      auditTopic = config.getString(MessageConsumerFactory.TOPIC_NAME_KEY);
      // set consumer end time as auditEndTime
      // adding 2 minutes to the end time so that
      // 1)when formatter converts it to minute level granularity we get the
      // ceiling instead of floor;for eg:
      // messages produced at 2:30:12 would only be considered in audit query
      // when end time is 2:31 and not 2:30
      // 2) Since the audit query doesn't include the upper bound hence adding 1
      // more minute

      Calendar calendar = Calendar.getInstance();
      calendar.setTime(benchmark.getConsumerEndTime());
      calendar.add(Calendar.MINUTE, 2);
      auditEndTime = formatter.format(calendar.getTime());
    }
    int exitcode = 0;
    if (runConsumer) {
      // start audit thread to perform audit query
      AuditThread auditThread = createAuditThread(auditTopic, auditStartTime,
          auditEndTime, auditTimeout, maxSent * numProducers);
      auditThread.start();

      // wait for audit thread to join
      assert (auditThread != null);
      auditThread.join(consumerTimeout * 1000);
      System.out.println("Audit thread state: " + auditThread.getState());

      if (!auditThread.success) {
        System.out.println("Audit validation FAILED!");
      } else {
        System.out.println("Audit validation SUCCESS!");
      }
    }
    return exitcode;
  }

  static AuditThread createAuditThread(String topic, String fromTime,
      String toTime, int timeout, long maxMessages) {
    return new AuditThread(topic, fromTime, toTime, timeout, maxMessages);
  }

  static class AuditThread extends Thread {
    final String topic;
    final String startTime;
    final String endTime;
    final int timeout;
    final long maxMessages;
    boolean success = false;

    AuditThread(String topic, String startTime, String endTime, int timeout,
        long maxMessages) {
      this.topic = topic;
      this.startTime = startTime;
      this.endTime = endTime;
      this.timeout = timeout;
      this.maxMessages = maxMessages;
    }

    @Override
    public void run() {
      System.out.println("Audit Thread started!");
      System.out.println("Waiting for " + timeout
          + " secs before firing the query");
      try {
        Thread.sleep(timeout * 1000);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      AuditDbQuery auditQuery = new AuditDbQuery(endTime, startTime, "TOPIC="
          + topic, "TIER", null);

      try {
        auditQuery.execute();
      } catch (Exception e) {
        System.out.println("Audit Query execute failed with exception: "
            + e.getMessage());
        e.printStackTrace();
        return;
      }

      System.out.println("Displaying results for Audit Query: " + auditQuery);
      // display audit query results
      auditQuery.displayResults();

      // validate that all tiers have received same number of messages equal to
      // maxMessages
      Collection<Long> recvdMessages = auditQuery.getReceived().values();
      boolean match = true;
      for (Long msgCount : recvdMessages) {
        if (msgCount != maxMessages) {
          match = false;
          break;
        }
      }
      if (match == true) {
        success = true;
      }

      System.out.println("Audit Thread closed");
    }
  }

}
