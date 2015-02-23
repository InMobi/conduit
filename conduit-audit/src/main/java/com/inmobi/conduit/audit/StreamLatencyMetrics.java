package com.inmobi.conduit.audit;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import com.inmobi.conduit.audit.query.AuditDbQuery;
import com.inmobi.messaging.util.AuditUtil;

public class StreamLatencyMetrics {

  private List<String> streamList = new ArrayList<String>();
  private List<String> clusterList = new ArrayList<String>();
  private Map<String, StringBuilder> mailMessagePerLocalCluster = new HashMap<String, StringBuilder>();
  private Map<String, StringBuilder> mailMessagePerMergeCluster = new HashMap<String, StringBuilder>();
  private StringBuilder htmlBody = new StringBuilder();

  private Map<String, String> clusterUrlMap = new HashMap<String, String>();

  public static final String METRIC_DATE_FORMAT = "yyyy-MM-dd-HH";

  private static final Log LOG = LogFactory.getLog(StreamLatencyMetrics.class);

  public static final ThreadLocal<SimpleDateFormat> metric_formatter =
      new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat(METRIC_DATE_FORMAT);
    }
  };

  public static final ThreadLocal<SimpleDateFormat> formatter =
      new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat(AuditUtil.DATE_FORMAT);
    }
  };

  private void evaluateLatencies(String streamStr, String clusterStr, String urlStr,
      String percentileStr, int days, int hours,
      int relativeEndTimeInDays, int relativeEndTimeInHours, boolean sendMail) {
    if (percentileStr == null || percentileStr.isEmpty()) {
      percentileStr = "95,99";
    }
    LOG.info(" percentile str " + percentileStr);

    String [] streamSplits = streamStr.split(",");
    String [] clusterSplits = clusterStr.split(",");
    String [] urlSplits = urlStr.split(",");

    for (int i = 0; i < clusterSplits.length; i++) {
      clusterUrlMap.put(clusterSplits[i], urlSplits[i]);
    }
    LOG.info(" clusterUrl map " + clusterUrlMap);
    for (String stream : streamSplits) {
      streamList.add(stream);
    }
    LOG.info(" streams list " + streamList);
    for (String cluster : clusterSplits) {
      clusterList.add(cluster);
    }
    LOG.info(" clusters list " + clusterList);

    Calendar cal = getTimeToHour(days, hours);
    Date startDate = cal.getTime();

    Date endDate = getTimeToHour(relativeEndTimeInDays, relativeEndTimeInHours).getTime();

    String startTimeStr = formatter.get().format(startDate);
    String endTimeStr = formatter.get().format(endDate);
    String metricValueStr = metric_formatter.get().format(startDate);

    /*
     * rajub@tzns4003:~$ /usr/local/conduit-audit/bin/audit-client audit -group TIER,TOPIC -filter topic=beacon_rr_uj1_cpm_render,TIER=LOCAL -percentile 95 19-11-2014-08:00 19-11-2014-09:00  --conf /usr/local/conduit-audit/prod/conf/
     *
     * Displaying results for AuditStatsQuery [fromTime=19-11 08:00, toTime=19-11 09:00, groupBy=GroupBy[TIER, TOPIC], filter=Filter{TOPIC=[beacon_rr_uj1_cpm_render], TIER=[LOCAL]}, timeZone=null, percentiles=95]
[{"TOPIC":"beacon_rr_uj1_cpm_render","CLUSTER":null,"Received":698899,"HOSTNAME":null,"TIMEINTERVAL":null,"Latencies":{"95.0":3},"TIER":"LOCAL"}]
     */
    htmlBody.append("<html> \n<head> <style>table, th, td {border: 2px solid black;}"
        + "</style> \n <title> Local, Merge stream Delays </title>\n</head>  \n");

    // query the results and post/mail the results
    for (String cluster : clusterList) {
      ClusterHtml clusterHtml = new ClusterHtml(cluster);
      AuditDbQuery auditQuery = new AuditDbQuery(endTimeStr, startTimeStr,
          "TIER='LOCAL|MERGE',CLUSTER=" + cluster,
          "TIER,TOPIC", "GMT", percentileStr);
      try {
        auditQuery.execute();
        LOG.info(" displaying the results : ");
        auditQuery.displayResults();
      } catch (Exception e) {
        LOG.info("Audit Query execute failed with exception: "
            + e.getMessage());
        e.printStackTrace();
        return;
      }
      if (sendMail) {
        LOG.info("mail the latency metrics " + metricValueStr);
        mailLatencyMetrics(auditQuery, metricValueStr, clusterUrlMap.get(cluster), clusterHtml);

      } else {
        try {
          postOnlyLatencyMetrics(auditQuery, metricValueStr, clusterUrlMap.get(cluster), clusterHtml);
        } catch (JSONException e) {
          LOG.info("Json Exception occured while trying to post the metrics for " + cluster);
        }
      }
      if (sendMail) {
        htmlBody.append("<p></p><table BORDER=2 CELLPADDING=10> \n").append("<caption><b>").
            append(cluster+ " Latencies in minutes </b>").append("</caption>").
            append("<tr> \n <th>Cluster</th><th>StreamType</th> \n<th>Topic</th>\n");
        for (String percentile : percentileStr.split(",")) {
          htmlBody.append("<th col>"+ Double.parseDouble(percentile) +" percentile</th>").append("\n");
        }
        htmlBody.append(clusterHtml.getLocalHtmlBody()).append("\n").append(clusterHtml.getMergeHtmlBody());
        htmlBody.append("</table> ");
      }
    }
    if (sendMail) {
      htmlBody.append("</html>");
    }
  }

  private void mailLatencyMetrics(AuditDbQuery auditQuery,
      String startTimeStr, String url, ClusterHtml clusterHtml) {
    Set<Tuple> tupleSet = auditQuery.getTupleSet();
    Map<Tuple, Map<Float, Integer>> percentileTupleMap = auditQuery.getPercentile();
    String cluster = clusterHtml.getCluster();
    int localRowCount = 0;
    int mergeRowCount = 0;
    for (Tuple tuple : tupleSet) {
      if (streamList.contains(tuple.getTopic())) {
        Map<Float, Integer> percentileMap = percentileTupleMap.get(tuple);
        Set<Float> percentileStr = auditQuery.getPercentileSet();
        if (tuple.getTier().equalsIgnoreCase("LOCAL")) {
          if (!mailMessagePerLocalCluster.containsKey(cluster)) {
            clusterHtml.prepareLocalTableRowData(tuple.getTopic());
            mailMessagePerLocalCluster.put(cluster, new StringBuilder(tuple.getTopic()));
          } else {
            clusterHtml.updateLocalHtmlBodyWithRows(tuple.getTopic());
            mailMessagePerLocalCluster.get(cluster).append("\n           " + tuple.getTopic());
          }
          localRowCount++;
        }
        if (tuple.getTier().equalsIgnoreCase("MERGE")) {
          if (!mailMessagePerMergeCluster.containsKey(cluster)) {
            clusterHtml.prepareMergeTableRowData(tuple.getTopic());
            mailMessagePerMergeCluster.put(cluster, new StringBuilder(tuple.getTopic()));
          } else {
            clusterHtml.updateMergeHtmlBodyWithRows(tuple.getTopic());
            mailMessagePerMergeCluster.get(cluster).append("\n            " + tuple.getTopic());
          }
          mergeRowCount++;
        }
        for (Float percentileVal : percentileStr) {
          Integer latencyValue = percentileMap.get(percentileVal);

          if (tuple.getTier().equalsIgnoreCase("LOCAL")) {
            if (!mailMessagePerLocalCluster.containsKey(cluster)) {
              mailMessagePerLocalCluster.put(cluster, new StringBuilder(tuple.getTopic()));
            }

            clusterHtml.prepareLocalTableRowData(String.valueOf(latencyValue));
          } else if (tuple.getTier().equalsIgnoreCase("MERGE")) {
            if (!mailMessagePerMergeCluster.containsKey(cluster)) {
              mailMessagePerMergeCluster.put(cluster, new StringBuilder(tuple.getTopic()));
            }
            clusterHtml.prepareMergeTableRowData(String.valueOf(latencyValue));
          }
        }
      }
    }
    LOG.info(" cluster   " + cluster + "   lcoal row sspan count: " + localRowCount + "   merge row span count: " + mergeRowCount);
    clusterHtml.updateLocalMergeHtmlWithRowSpan(cluster, localRowCount, mergeRowCount);
  }

  /*
   * It prepares a result in json form and post it to the url
   */
  private void postOnlyLatencyMetrics(AuditDbQuery auditQuery,
      String startTimeStr, String url, ClusterHtml clusterHtml) throws JSONException {
    Set<Tuple> tupleSet = auditQuery.getTupleSet();
    Map<Tuple, Map<Float, Integer>> percentileTupleMap = auditQuery.getPercentile();
    JSONObject resultJson = new JSONObject();
    resultJson.put("x", startTimeStr);
    for (Tuple tuple : tupleSet) {
      if (streamList.contains(tuple.getTopic())) {
        Map<Float, Integer> percentileMap = percentileTupleMap.get(tuple);
        Set<Float> percentileStr = auditQuery.getPercentileSet();
        for (Float percentileVal : percentileStr) {
          Integer latencyValue = percentileMap.get(percentileVal);
          String percentile = String.valueOf(percentileVal).substring(0,
              String.valueOf(percentileVal).indexOf('.'));
          resultJson.put(tuple.getTier() + " " + percentile + " " + tuple.getTopic() , latencyValue);
        }
      }
    }
    LOG.info("Posting the resultJson : " + resultJson + " on url " + url);
    try {
      postLatencies(resultJson, url);
    } catch (IOException e) {
      LOG.info("IOException occured while trying to post the metrics to " + url + " : ", e);
    }
  }

  private void sendMail(String mailIdList) {
    List<String> emailIdList = new ArrayList<String>();
    for (String mailId : mailIdList.split(",")) {
      emailIdList.add(mailId);
    }
    EmailHelper.sendMail(htmlBody.toString(), emailIdList);
  }

  private void postLatencies(JSONObject resultJson, String url) throws IOException {
    HttpURLConnection con = null;
    try {
      con = (HttpURLConnection) ((new URL(url).openConnection()));
      con.setDoOutput(true);
      con.setDoInput(true);
      con.setUseCaches(false);

      con.connect();
      LOG.info("posting the resultJson    : " + resultJson + "  to  url " + con);
      DataOutputStream wr = new DataOutputStream(con.getOutputStream());
      wr.writeBytes("log=" + resultJson.toString());
      wr.flush();
      wr.close();

      LOG.info("reading from the url using input stream ");
      BufferedReader in = new BufferedReader(
          new InputStreamReader(con.getInputStream()));
      String decodedString;
      while ((decodedString = in.readLine()) != null) {
        LOG.info("decoded response : " + decodedString);
      }
      in.close();
    } finally {
      if (con != null) {
        LOG.info("disconnecting to bedner " );
        con.disconnect();
      }
    }
  }

  private Calendar getTimeToHour(int days, int hours) {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, -days);
    cal.add(Calendar.HOUR_OF_DAY, -hours);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal;
  }

  private static void printUsage() {
    System.out.println("Usage: ");
    System.out.println(
        "[-streams (comma separated stream names)]" + "[-clusters (comma seaprated list) ]"
            +"[-percentile (comma separated)]" + "[-days (relative time from current time (days from now))]"
            + "[-hours (number of hours beyond from now)]");
  }

  public static void main(String[] args) {
    String streamStr = null;
    String clusterStr = null;
    String percentileStr = null;
    String urlStr = null;
    int relativeStartTimeInHours = 6;
    int relativeStartTimeInDays = 0;
    int relativeEndTimeInHours = 5;
    int relativeEndTimeInDays = 0;
    Boolean sendWeeklyMail = false;
    String mailIdList = "raju.bairishetti@inmobi.com";
    if (args.length < 3) {
      printUsage();
      System.exit(-1);
    }
    for (int i = 0; i < args.length;) {
      LOG.info(" parsing inputs args " + args);
      if (args[i].equalsIgnoreCase("-streams")) {
        streamStr = args[i+1];
        i += 2;
      } else if (args[i].equalsIgnoreCase("-clusters")) {
        clusterStr = args[i+1];
        i += 2;
      } else if (args[i].equalsIgnoreCase("-urls")) {
        urlStr = args[i + 1];
        i += 2;
      }else if (args[i].equalsIgnoreCase("-percentile")) {
        percentileStr = args[i + 1];
        i += 2;
      } else if (args[i].equalsIgnoreCase("-hours")) {
        relativeStartTimeInHours = Integer.parseInt(args[i+1]);
        i += 2;
      } else if (args[i].equalsIgnoreCase("-days")) {
        relativeStartTimeInDays = Integer.parseInt(args[i+1]);
        i += 2;
      } else if (args[i].equalsIgnoreCase("-endTimeInHours")) {
        relativeEndTimeInHours = Integer.parseInt(args[i+1]);
        i += 2;
      } else if (args[i].equalsIgnoreCase("-endTimeInDays")) {
        relativeEndTimeInDays = Integer.parseInt(args[i+1]);
        i += 2;
      } else if (args[i].equalsIgnoreCase("-sendMail")) {
        sendWeeklyMail = Boolean.parseBoolean(args[i+1]);
        i += 2;
      } else if (args[i].equalsIgnoreCase("-mailIdList")) {
        mailIdList = args[i+1];
        i += 2;
      }
      else {
        printUsage();
      }
    }
    //TODO Validate parameters
    StreamLatencyMetrics latencyMetrics = new StreamLatencyMetrics();
    latencyMetrics.evaluateLatencies(streamStr, clusterStr, urlStr,
        percentileStr, relativeStartTimeInDays, relativeStartTimeInHours,
        relativeEndTimeInDays, relativeEndTimeInHours, sendWeeklyMail);
    if (sendWeeklyMail) {
      latencyMetrics.sendMail(mailIdList);
    }

  }
  
}