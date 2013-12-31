package com.inmobi.databus.audit.query;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import com.inmobi.databus.audit.util.AuditDBConstants;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.json.JSONException;
import org.json.JSONObject;

import com.inmobi.databus.audit.Filter;
import com.inmobi.databus.audit.GroupBy;
import com.inmobi.databus.audit.Tuple;
import com.inmobi.databus.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.util.AuditUtil;

/**
 * A Query wrapper to get databus audit points for a selected time range to
 * construct a timeseries graph
 * 
 */
public class AuditTimeLineDbQuery {

  private static final String DEFAULT_TIMEZONE = "GMT";
  private static final Log LOG = LogFactory.getLog(AuditTimeLineDbQuery.class);

  private String timeZone, filterString, groupByString, toTimeString,
      fromTimeString;

  Date fromTime;
  Date toTime;
  GroupBy groupBy;
  Filter filter;
  Set<Tuple> tupleSet;
  private final ClientConfig config;

  public AuditTimeLineDbQuery(String toTimeString, String fromTimeString,
      String filterString, String groupByString, String timeZone) {
    this(toTimeString, fromTimeString, filterString, groupByString, timeZone,
        null);
  }

  public AuditTimeLineDbQuery(String toTimeString, String fromTimeString,
      String filterString, String groupByString, String timeZone,
      ClientConfig config) {
    tupleSet = new HashSet<Tuple>();
    this.toTimeString = toTimeString;
    this.fromTimeString = fromTimeString;
    this.filterString = filterString;
    this.groupByString = groupByString;
    this.timeZone = timeZone;
    if (config != null)
      this.config = config;
    else {
      this.config =
          ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE);
    }

  }

  void executeQuery() {
    LOG.debug("To time:" + toTime);
    LOG.debug("From time:" + fromTime);
    AuditDBHelper dbHelper = new AuditDBHelper(config);
    Set<Tuple> tuples =
        dbHelper.retrieveTimeSeries(fromTime, toTime, filter, groupBy);
    if (tuples != null) {
      tupleSet.addAll(tuples);
    } else {
      LOG.error("Tupleset retrieved is null, error in helper retireve() "
          + "method");
      return;
    }
    LOG.debug("Tuple set retrieved from DB: " + tupleSet);
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
    executeQuery();
  }

  void parseAndSetArguments() throws ParseException, IOException {
    groupBy = new GroupBy(groupByString);
    filter = new Filter(filterString);
    fromTime = getDate(fromTimeString);
    toTime = getDate(toTimeString);
  }

  @Override
  public String toString() {
    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM HH:mm");
    return "AuditStatsQuery [fromTime=" + formatter.format(fromTime)
        + ", toTime=" + formatter.format(toTime) + ", groupBy=" + groupBy
        + "timeZone=" + timeZone +  "]";
  }

  public void displayResults() throws JSONException {
    System.out.println(convertToJson());
  }

  public Set<Tuple> getTupleSet() {
    return Collections.unmodifiableSet(tupleSet);
  }

  @SuppressWarnings("unchecked")
  public Map<String, Map<String, Map<String, Object>>> convertToMap() {
    Map<String, Map<String, Map<String, Object>>> map =
        new HashMap<String, Map<String, Map<String, Object>>>();

    for (Tuple eachTuple : tupleSet) {
      Map<String, Map<String, Object>> eachTierMap =
          map.get(eachTuple.getTier());
      if (eachTierMap == null) {
        eachTierMap = new HashMap<String, Map<String, Object>>();
        map.put(eachTuple.getTier(), eachTierMap);
      }
      Map<String, Object> eachTSMap =
          eachTierMap.get("" + eachTuple.getTimestamp().getTime());
      if (eachTSMap == null) {
        eachTSMap = new HashMap<String, Object>();
        eachTierMap.put("" + eachTuple.getTimestamp().getTime(), eachTSMap);
        eachTSMap.put("aggreceived", eachTuple.getReceived());
        eachTSMap.put("aggsent", eachTuple.getSent());
      } else {
        eachTSMap.put("aggreceived", (Long) eachTSMap.get("aggreceived")
            + eachTuple.getReceived());
        eachTSMap.put("aggsent",
            (Long) eachTSMap.get("aggsent") + eachTuple.getSent());
      }
      Map<String, Object> eachStream =
          (Map<String, Object>) eachTSMap.get(eachTuple.getTopic());
      if (eachStream == null) {
        eachStream = new HashMap<String, Object>();
        eachTSMap.put(eachTuple.getTopic(), eachStream);
        eachStream.put("topic", eachTuple.getTopic());
      }

      List<Map<String, Object>> clusterList =
          (List<Map<String, Object>>) eachStream.get("clusterStats");
      if (clusterList == null) {
        clusterList = new ArrayList<Map<String, Object>>();
        eachStream.put("clusterStats", clusterList);
      }
      Map<String, Object> clusterStat = new HashMap<String, Object>();
      clusterStat.put("cluster", eachTuple.getCluster());
      clusterStat.put("received", eachTuple.getReceived());
      clusterStat.put("sent", eachTuple.getSent());
      clusterList.add(clusterStat);

    }
    return map;

  }

  @SuppressWarnings(value = { "rawtypes", "unchecked" })
  public Map<String, Object> covertToUIFriendlyObject() {
    Map<String, Object> returnMap = new HashMap<String, Object>();
    Map<String, Map<String, Map<String, Object>>> map = convertToMap();

    List<Object> datapoints = new ArrayList<Object>();
    for (String eachTier : map.keySet()) {
      Map<String, Object> eachTierMap = new HashMap<String, Object>();
      eachTierMap.put("tier", eachTier);
      List modifiedtimeserierList = new ArrayList();
      eachTierMap.put("tierWisePointList", modifiedtimeserierList);
      for (String eachTimeKey : map.get(eachTier).keySet()) {
        Object eachTimeData = map.get(eachTier).get(eachTimeKey);
        Map<String, Object> eachObject = (Map<String, Object>) eachTimeData;
        List<Object> steamwiseMap = new ArrayList<Object>();
        Map<String, Object> changedMap = new HashMap<String, Object>();
        Set<String> keys = eachObject.keySet();
        for (String eachKey : keys) {
          if (!eachKey.equals("aggreceived") && !eachKey.equals("aggsent")) {

            steamwiseMap.add(eachObject.get(eachKey));
          } else {
            changedMap.put(eachKey, eachObject.get(eachKey));
          }
        }
        changedMap.put("time", eachTimeKey);
        changedMap.put("topicCountList", steamwiseMap);
        modifiedtimeserierList.add(changedMap);
        System.out.println(eachTimeData);
      }
      datapoints.add(eachTierMap);
    }
    returnMap.put("datapoints", datapoints);

    return returnMap;

  }

  public String convertToJson() {
    JSONObject newObject = new JSONObject(covertToUIFriendlyObject());
    return newObject.toString();

  }

  public static void main(String args[]) throws Exception {
    AuditTimeLineDbQuery query =
        new AuditTimeLineDbQuery("02-12-2013-01:00", "04-12-2013-02:00", "",
            "TIER,TOPIC,CLUSTER,TIMEINTERVAL", "GMT");
    query.execute();
    query.convertToMap();
    System.out.println(query.convertToJson());
    System.out.println(query.getTupleSet().size());
  }

}
