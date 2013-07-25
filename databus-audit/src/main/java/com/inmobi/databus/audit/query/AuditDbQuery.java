package com.inmobi.databus.audit.query;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.inmobi.databus.audit.AuditStats;
import com.inmobi.databus.audit.Column;
import com.inmobi.databus.audit.Filter;
import com.inmobi.databus.audit.GroupBy;
import com.inmobi.databus.audit.LatencyColumns;
import com.inmobi.databus.audit.Tuple;
import com.inmobi.databus.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.util.AuditUtil;

public class AuditDbQuery {

  private static final int minArgs = 2;
  private static final String DEFAULT_TIMEZONE = "GMT";
  private static final Log LOG = LogFactory.getLog(AuditDbQuery.class);

  private String timeZone, filterString, groupByString, toTimeString,
      fromTimeString, percentileString;

  Map<Tuple, Map<Float, Integer>> percentile = new HashMap<Tuple, Map<Float, Integer>>();
  Date fromTime;
  Date toTime;
  GroupBy groupBy;
  Filter filter;
  Set<Float> percentileSet;
  Set<Tuple> tupleSet;
  Map<GroupBy.Group, Long> received;
  Map<GroupBy.Group, Long> sent;
  private final ClientConfig config;

  public AuditDbQuery(String toTimeString, String fromTimeString,
      String filterString, String groupByString, String timeZone) {
    this(toTimeString, fromTimeString, filterString, groupByString, timeZone,
        null);
  }
  public AuditDbQuery(String toTimeString, String fromTimeString,
      String filterString, String groupByString, String timeZone,
      String percentileString) {
    this(toTimeString, fromTimeString, filterString, groupByString, timeZone,
        percentileString, null);
  }

  public AuditDbQuery(String toTimeString, String fromTimeString,
      String filterString, String groupByString, String timeZone,
      String percentileString, ClientConfig config) {
    received = new TreeMap<GroupBy.Group, Long>();
    sent = new TreeMap<GroupBy.Group, Long>();
    tupleSet = new HashSet<Tuple>();
    this.toTimeString = toTimeString;
    this.fromTimeString = fromTimeString;
    this.filterString = filterString;
    this.groupByString = groupByString;
    this.timeZone = timeZone;
    this.percentileString = percentileString;
    if (config != null)
      this.config = config;
    else {
      this.config = ClientConfig.loadFromClasspath(AuditStats.CONF_FILE);
    }

  }

  void aggregateStats() {
    LOG.debug("To time:" + toTime);
    LOG.debug("From time:" + fromTime);
    AuditDBHelper dbHelper = new AuditDBHelper(config);
    tupleSet.addAll(dbHelper.retrieve(toTime, fromTime, filter, groupBy));
    LOG.debug("Tuple set retrieved from DB: " + tupleSet);
    setReceivedAndSentStats();
    if (percentileSet != null) {
      LOG.debug("Creating percentile map for all tuples");
      populatePercentileMap();
    }
  }

  private void setReceivedAndSentStats() {
    for (Tuple tuple : tupleSet) {
      if (!tuple.isGroupBySet())
        tuple.setGroupBy(groupBy);
      GroupBy.Group group = tuple.getGroup();
      received.put(group, tuple.getReceived());
      sent.put(group, tuple.getSent());
    }
  }

  private void populatePercentileMap() {
    for (Tuple tuple : tupleSet) {
      LOG.debug("Creating percentile map for tuple :" + tuple.toString());
      Long totalCount = tuple.getReceived() - tuple.getLostCount();
      Long currentCount = 0l;
      Iterator<Float> it = percentileSet.iterator();
      Float currentPercentile = it.next();
      for (LatencyColumns latencyColumn : LatencyColumns.values()) {
        if (latencyColumn == LatencyColumns.C600)
          continue;
        Long value = tuple.getLatencyCountMap().get(latencyColumn);
        while (currentCount + value >= ((currentPercentile * totalCount) / 100)) {
          Map<Float, Integer> percentileMap = percentile.get(tuple);
          if (percentileMap == null)
            percentileMap = new HashMap<Float, Integer>();
          percentileMap.put(currentPercentile, latencyColumn.getValue());
          percentile.put(tuple, percentileMap);
          if (it.hasNext())
            currentPercentile = it.next();
          else
            break;
        }
        if (!it.hasNext() && percentile.get(tuple) != null
            && percentile.get(tuple).get(currentPercentile) != null)
          break;
        currentCount += value;
      }
    }
  }

  private Date getDate(String date) throws ParseException {
    SimpleDateFormat formatter = new SimpleDateFormat(AuditUtil.DATE_FORMAT);
    if (timeZone != null && !timeZone.isEmpty()) {
      formatter.setTimeZone(TimeZone.getTimeZone(timeZone));
    } else {
      formatter.setTimeZone(TimeZone.getTimeZone(DEFAULT_TIMEZONE));
    }
    return formatter.parse(date);
  }

  public void execute() throws ParseException, IOException,
      InterruptedException, TException {
    parseAndSetArguments();
    aggregateStats();
  }

  void parseAndSetArguments() throws ParseException, IOException {
    groupBy = new GroupBy(groupByString);
    filter = new Filter(filterString);
    fromTime = getDate(fromTimeString);
    toTime = getDate(toTimeString);
    percentileSet = getPercentileList(percentileString);
  }

  private Set<Float> getPercentileList(String percentileString) {
    if (percentileString != null && !percentileString.isEmpty()) {
      Set<Float> percentileSet = new TreeSet<Float>();
      String[] percentiles = percentileString.split(",");
      for (String percentile : percentiles)
        percentileSet.add(Float.parseFloat(percentile));
      return percentileSet;
    }
    return null;
  }

  public static void main(String args[]) {
    String groupByKeys = null;
    String filterKeys = null;
    String timeZone = null;
    String fromTime = null, toTime = null;
    String percentileString = null;
    try {
      if (args.length < minArgs) {
        printUsage();
        return;
      }
      for (int i = 0; i < args.length;) {
        if (args[i].equalsIgnoreCase("-group")) {
          groupByKeys = args[i + 1];
          LOG.info("Group is " + groupByKeys);
          i = i + 2;
        } else if (args[i].equalsIgnoreCase("-filter")) {
          filterKeys = args[i + 1];
          LOG.info("Filter is " + filterKeys);
          i = i + 2;
        } else if (args[i].equalsIgnoreCase("-timezone")) {
          timeZone = args[i + 1];
          LOG.info("TimeZone is " + timeZone);
          i = i + 2;
        } else if (args[i].equalsIgnoreCase("-percentile")) {
          percentileString = args[i + 1];
          i = i + 2;
        } else {
          if (fromTime == null) {
            fromTime = args[i++];
            LOG.info("From time is " + fromTime);
          } else {
            toTime = args[i++];
            LOG.info("To time is " + toTime);
          }
        }
      }
      if (fromTime == null || toTime == null) {
        printUsage();
        System.exit(-1);
      }
      AuditDbQuery auditQuery = new AuditDbQuery(toTime, fromTime, filterKeys,
          groupByKeys, timeZone, percentileString);
      try {
        auditQuery.execute();
      } catch (InterruptedException e) {
        LOG.error("Exception in query", e);
        System.exit(-1);
      } catch (TException e) {
        LOG.error("Exception in query", e);
        System.exit(-1);
      }
      System.out.println("Displaying results for " + auditQuery);
      auditQuery.displayResults();
    } catch (Throwable e) {
      LOG.error("Runtime Exception", e);
      System.exit(-1);
    }
  }

  @Override
  public String toString() {
    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM HH:mm");
    return "AuditStatsQuery [fromTime=" + formatter.format(fromTime)
        + ", toTime=" + formatter.format(toTime) + ", groupBy=" + groupBy
        + ", filter=" + filter + ", timeZone=" + timeZone + ", "
        + "percentiles=" + percentileString + "]";
  }

  public void displayResults() {
    StringBuffer results = new StringBuffer();
    results.append("Group \t\t\tReceived\t\t\t<Percentile, Latency>\n");
    for (Tuple tuple : tupleSet) {
      results.append(tuple.getGroup() + "\t\t");
      results.append(received.get(tuple.getGroup()) + "\t\t");
      Map<Float, Integer> percentileMap = percentile.get(tuple);
      if (percentileMap != null) {
        for (Map.Entry<Float, Integer> percentileEntry : percentileMap
            .entrySet()) {
          results.append("<" + percentileEntry.getKey() + ", ");
          results.append(percentileEntry.getValue() + ">\t");
        }
      }
      results.append("\n");
    }
    System.out.println(results);
  }

  private static void printUsage() {
    StringBuffer usage = new StringBuffer();
    usage.append("Usage : AuditDbQuery ");
    usage.append("[-group <comma seperated columns>]");
    usage.append("[-filter <comma seperated column=<value>>]");
    usage.append("where column can take value :[");
    for (Column key : Column.values()) {
      usage.append(key);
      usage.append(",");
    }
    usage.append("]");
    usage.append("[-timezone]");
    usage.append("[-percentile <comma seperated percentile>]");
    usage.append("fromTime(" + AuditUtil.DATE_FORMAT + ")" + "toTime("
        + AuditUtil.DATE_FORMAT + ")");
    System.out.println(usage);
  }

  @Deprecated
  public Map<GroupBy.Group, Long> getReceived() {
    return received;
  }

  @Deprecated
  public Map<GroupBy.Group, Long> getSent() {
    return sent;
  }

  public Map<Tuple, Map<Float, Integer>> getPercentile() {
    return percentile;
  }

  public Set<Tuple> getTupleSet() {
    return Collections.unmodifiableSet(tupleSet);
  }

  public Set<Float> getPercentileSet() {
    return percentileSet;
  }
}
