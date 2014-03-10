package com.inmobi.conduit.visualization.server.util;

import com.google.protobuf.gwt.server.ServerJsonStreamFactory;
import com.inmobi.conduit.audit.Column;
import com.inmobi.conduit.audit.GroupBy;
import com.inmobi.conduit.visualization.server.MessageStats;
import com.inmobi.conduit.visualization.server.Node;
import com.inmobi.conduit.visualization.server.NodeKey;
import com.inmobi.conduit.visualization.server.ServerConstants;
import com.inmobi.conduit.visualization.server.VisualizationProperties;
import com.inmobi.conduit.visualization.shared.CommonConstants;
import com.inmobi.conduit.visualization.shared.RequestResponse;
import com.inmobi.conduit.audit.LatencyColumns;
import com.inmobi.conduit.audit.Tuple;
import com.inmobi.conduit.audit.query.AuditDbQuery;
import com.inmobi.messaging.ClientConfig;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class ServerDataHelper {
  private static Logger LOG = Logger.getLogger(ServerDataHelper.class);
  private static ServerDataHelper severDataHelperInstance;

  public static ServerDataHelper getInstance() {
    if (severDataHelperInstance == null) {
      severDataHelperInstance = new ServerDataHelper();
    }
    return severDataHelperInstance;
  }

  public String getStreamFromGraphDataReq(String clientJson) {
    String stream = null;
    try {
      stream = RequestResponse.Request.newBuilder().readFrom(
          ServerJsonStreamFactory.getInstance()
              .createNewStreamFromJson(clientJson)).build()
          .getGraphDataRequest().getStream();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return stream;
  }

  public String getColoFromGraphDataReq(String clientJson) {
    String colo = null;
    try {
      colo = RequestResponse.Request.newBuilder().readFrom(
          ServerJsonStreamFactory.getInstance()
              .createNewStreamFromJson(clientJson)).build()
          .getGraphDataRequest().getColo();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return colo;
  }

  public String getStartTimeFromGraphDataReq(String clientJson) {
    String startTime = null;
    try {
      startTime = RequestResponse.Request.newBuilder().readFrom(
          ServerJsonStreamFactory.getInstance()
              .createNewStreamFromJson(clientJson)).build()
          .getGraphDataRequest().getStartTime();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return startTime;
  }

  public String getEndTimeFromGraphDataReq(String clientJson) {
    String endTime = null;
    try {
      endTime = RequestResponse.Request.newBuilder().readFrom(
          ServerJsonStreamFactory.getInstance()
              .createNewStreamFromJson(clientJson)).build()
          .getGraphDataRequest().getEndTime();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return endTime;
  }

  public String setTopologyDataResponse(Map<NodeKey, Node> nodeMap,
                                        Map<String, String> filterMap) {
    JSONObject newObject = new JSONObject();
    JSONArray nodeArray = new JSONArray();
    try {
      LOG.debug("Parsing the nodeList into a JSON");
      for (Node node : nodeMap.values()) {
        LOG.debug("Parsing node : " + node.toString());
        JSONObject nodeObject = new JSONObject();
        nodeObject.put("name", node.getName());
        nodeObject.put("cluster", node.getClusterName());
        nodeObject.put("tier", node.getTier());
        nodeObject.put("aggregatereceived",
            String.valueOf(node.getAggregateMessagesReceived()));
        JSONArray topicStatsArray = new JSONArray();
        if (node.getReceivedMessagesList() != null) {
          for (MessageStats messagestat : node.getReceivedMessagesList()) {
            JSONObject topicStatsObject = new JSONObject();
            topicStatsObject.put("topic", messagestat.getTopic());
            topicStatsObject.put("messages", messagestat.getMessages());
            topicStatsObject.put("hostname", messagestat.getHostname());
            topicStatsArray.put(topicStatsObject);
          }
        }
        nodeObject.put("receivedtopicStatsList", topicStatsArray);
        nodeObject.put("aggregatesent",
            String.valueOf(node.getAggregateMessagesSent()));
        JSONArray senttopicStatsArray = new JSONArray();
        if (node.getSentMessagesList() != null) {
          for (MessageStats messagestat : node.getSentMessagesList()) {
            JSONObject topicStatsObject = new JSONObject();
            topicStatsObject.put("topic", messagestat.getTopic());
            topicStatsObject.put("messages", messagestat.getMessages());
            topicStatsObject.put("hostname", messagestat.getHostname());
            senttopicStatsArray.put(topicStatsObject);
          }
        }
        nodeObject.put("senttopicStatsList", senttopicStatsArray);
        if ((node.getTier().equalsIgnoreCase("merge") ||
            node.getTier().equalsIgnoreCase("mirror")) &&
            node.getSourceList() != null) {

          JSONArray steamSourceArray = new JSONArray();
          for (Map.Entry<String, Set<String>> sourceEntry: node
              .getTopicSourceList().entrySet()) {
            JSONObject object = new JSONObject();
            object.put("topic", sourceEntry.getKey());
            object.put("sourceList", sourceEntry.getValue());
            steamSourceArray.put(object);
          }
          nodeObject.put("topicSource", steamSourceArray);
          JSONArray sourceListArray = new JSONArray();
          sourceListArray.put(node.getSourceList());
          nodeObject.put("source", sourceListArray);
        }
        JSONArray percentileArray = new JSONArray();
        for(Map.Entry<Float, Integer> entry : node.getPercentileMap()
            .entrySet()) {
          JSONObject percentileObject = new JSONObject();
          percentileObject.put("percentile", entry.getKey());
          percentileObject.put("latency", entry.getValue());
          percentileArray.put(percentileObject);
        }
        nodeObject.put("overallLatency", percentileArray);
        JSONArray topicPercentileArray = new JSONArray();
        for(Map.Entry<String, Map<Float, Integer>> entry : node.getPerTopicPercentileMap().entrySet()) {
          JSONObject topicPercentileObject = new JSONObject();
          JSONArray topicLatency = new JSONArray();
          for (Map.Entry<Float, Integer> percentileEntry : entry.getValue()
              .entrySet()) {
            JSONObject percentileObject = new JSONObject();
            percentileObject.put("percentile", percentileEntry.getKey());
            percentileObject.put("latency", percentileEntry.getValue());
            topicLatency.put(percentileObject);
          }
          topicPercentileObject.put("topic", entry.getKey());
          topicPercentileObject.put("percentileLatencyList", topicLatency);
          topicPercentileArray.put(topicPercentileObject);
        }
        nodeObject.put("topicLatency", topicPercentileArray);
        LOG.debug("Parsed node JSON : " + nodeObject.toString());
        nodeArray.put(nodeObject);
      }
      newObject.put("nodes", nodeArray);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ServerJsonStreamFactory.getInstance().serializeMessage(
        RequestResponse.Response.newBuilder().setTopologyDataResponse(
            RequestResponse.TopologyDataResponse.newBuilder()
                .setJsonString(newObject.toString()).setRequestParams
                (getRequestParametersObject(filterMap))).build());
  }

  public String setTierLatencyResponseObject(Map<Tuple, Map<Float,
      Integer>> tierLatencyMap, VisualizationProperties properties,
                                             Map<String, String> filterMap) {
    Float percentileForSla = Float.valueOf(properties.get(ServerConstants
        .PERCENTILE_FOR_SLA));
    List<RequestResponse.TierLatencyObj> tierLatencyObjList = new ArrayList
        <RequestResponse.TierLatencyObj>();
    for (Map.Entry<Tuple, Map<Float, Integer>> tierEntry : tierLatencyMap
        .entrySet()) {
      String tier = tierEntry.getKey().getTier();
      int latency = tierEntry.getValue().get(percentileForSla);
      RequestResponse.TierLatencyObj newObj = RequestResponse.TierLatencyObj
          .newBuilder().setTier(tier).setLatency(latency).build();
      tierLatencyObjList.add(newObj);
    }
    return ServerJsonStreamFactory.getInstance().serializeMessage(
        RequestResponse.Response.newBuilder().setTierLatencyResponse(
            RequestResponse.TierLatencyResponse.newBuilder()
                .addAllTierLatencyObjList(tierLatencyObjList)
                .setRequestParams(getRequestParametersObject(filterMap))).build());
  }

  public String setLoadMainPanelResponse(List<String> streamList,
                                         List<String> clusterList,
                                         VisualizationProperties properties,
                                         ClientConfig feederConfig) {
    RequestResponse.ClientConfiguration clientConfiguration =
        RequestResponse.ClientConfiguration.newBuilder()
            .setPublisherSla(properties.get(ServerConstants.PUBLISHER_SLA))
            .setAgentSla(
                properties.get(ServerConstants.AGENT_SLA))
            .setVipSla(properties.get(ServerConstants.VIP_SLA))
            .setCollectorSla(properties.get(ServerConstants.COLLECTOR_SLA))
            .setHdfsSla(properties.get(ServerConstants.HDFS_SLA))
            .setPercentileForSla(properties
                .get(ServerConstants.PERCENTILE_FOR_SLA))
            .setPercentageForLoss(properties
                .get(ServerConstants.PERCENTAGE_FOR_LOSS))
            .setPercentageForWarn(properties
                .get(ServerConstants.PERCENTAGE_FOR_WARN))
            .setMaxStartTime(properties
                .get(ServerConstants.MAX_START_TIME))
            .setMaxTimeRangeInt(properties.get(ServerConstants
                .MAX_TIME_RANGE_INTERVAL_IN_HOURS))
            .setWarnLossThresholdDiff(properties
                .get(ServerConstants.LOSS_WARN_THRESHOLD_DIFF_IN_MINS))
            .setLocalSla(properties.get(ServerConstants.LOCAL_SLA))
            .setMergeSla(properties.get(ServerConstants.MERGE_SLA))
            .setMirrorSla(properties.get(ServerConstants.MIRROR_SLA))
            .setRolleduptilldays(feederConfig.getString(ServerConstants
                .ROLLEDUP_TILL_DAYS, "2"))
            .build();
    RequestResponse.LoadMainPanelResponse loadMainPanelResponse =
        RequestResponse.LoadMainPanelResponse.newBuilder()
            .addAllStream(streamList).addAllCluster(clusterList)
            .setClientConfig(clientConfiguration).build();
    return ServerJsonStreamFactory.getInstance().serializeMessage(
        RequestResponse.Response.newBuilder()
            .setLoadMainPanelResponse(loadMainPanelResponse).build());
  }

  public static String setTimeLineDataResponse(String jsonResult,
                                               Map<String, String> filterMap) {
    return ServerJsonStreamFactory.getInstance().serializeMessage(
        RequestResponse.Response.newBuilder().setTimeLineGraphResponse(
                RequestResponse.TimeLineGraphResponse.newBuilder()
                    .setJsonString(jsonResult).setRequestParams
                    (getRequestParametersObject(filterMap))).build());
  }

  private static RequestResponse.GraphDataRequest getRequestParametersObject
      (Map<String, String> filterMap) {
    return RequestResponse.GraphDataRequest.newBuilder().setStartTime(filterMap.get
            (CommonConstants.START_TIME_FILTER)).setEndTime(filterMap.get
            (CommonConstants.END_TIME_FILTER)).setColo(filterMap.get(
            CommonConstants.CLUSTER_FILTER)).setStream(filterMap.get
            (CommonConstants.STREAM_FILTER)).build();
  }
  
  private static final String TIER = "tier";
  private static final String TIERWISEPOINTLIST = "tierWisePointList";
  private static final String AGGRECEIVED = "aggreceived";
  private static final String AGGSENT = "aggsent";
  private static final String OVERALLLATENCY ="overallLatency";
  private static final String TIME = "time";
  private static final String CLUSTERCOUNTLIST = "clusterCountList";
  private static final String DATAPOINTS ="datapoints";
  private static final String PERCENTILE ="percentile";
  private static final String LATENCY ="latency";
  private static final String TOPIC ="topic";
  private static final String TOPICSTATS = "topicStats";
  private static final String CLUSTERLATENCY = "clusterLatency";
  private static final String CLUSTER= "cluster";
  private static final String RECEIVED = "received";
  private static final String SENT ="sent";
  private static final String TOPICLATENCY = "topicLatency";

  @SuppressWarnings("unchecked")
  private static Map<String, Map<String, Map<String, Object>>> convertToMap
      (AuditDbQuery query) {

    Map<String, Map<String, Map<String, Object>>> map = new HashMap<String, Map<String, Map<String, Object>>>();
    Map<Tuple, Map<Float, Integer>> percentileMap = query.getPercentile();

    Set<Tuple> allAggTupleSet = groupTuples(query.getTupleSet(),
        new GroupBy(ServerConstants.GROUPBY_ALL_AGG_TIMELINE_STR));
    Set<Tuple> clusterAggTupleSet = groupTuples(query.getTupleSet(),
        new GroupBy(ServerConstants.GROUPBY_CLUSTER_AGG_TIMELINE_STR));

    Map<Tuple, Map<Float, Integer>> allAggPercentileMap = AuditDbQuery
        .populatePercentileMap(allAggTupleSet, query.getPercentileSet());
    Map<Tuple, Map<Float, Integer>> clusterAggPercentileMap = AuditDbQuery
        .populatePercentileMap(clusterAggTupleSet, query.getPercentileSet());

    for (Tuple tuple : query.getTupleSet()) {
      Map<String, Map<String, Object>> eachTierMap =
          map.get(tuple.getTier());
      if (eachTierMap == null) {
        eachTierMap = new TreeMap<String, Map<String, Object>>();
        map.put(tuple.getTier(), eachTierMap);
      }
      Map<String, Object> eachTSMap =
          eachTierMap.get("" + tuple.getTimestamp().getTime());
      if (eachTSMap == null) {
        eachTSMap = new HashMap<String, Object>();
        eachTierMap.put("" + tuple.getTimestamp().getTime(), eachTSMap);
        eachTSMap.put(AGGRECEIVED, tuple.getReceived());
        eachTSMap.put(AGGSENT, tuple.getSent());
        eachTSMap.put(OVERALLLATENCY, convertTLatencyListOfMaps
            (allAggPercentileMap.get(new Tuple(tuple.getHostname(),
                tuple.getTier(), null, tuple.getTimestamp(), null)).entrySet()));
      } else {
        eachTSMap.put(AGGRECEIVED, (Long) eachTSMap.get(AGGRECEIVED)
            + tuple.getReceived());
        eachTSMap.put(AGGSENT, (Long) eachTSMap.get(AGGSENT) + tuple
            .getSent());
      }
      Map<String, Object> eachCluster = (Map<String, Object>) eachTSMap.get
          (tuple.getCluster());
      if (eachCluster == null) {
        eachCluster = new HashMap<String, Object>();
        eachTSMap.put(tuple.getCluster(), eachCluster);
        eachCluster.put(CLUSTER, tuple.getCluster());
      }
      List<Map<String, Object>> topicList =
          (List<Map<String, Object>>) eachCluster.get(TOPICSTATS);
      if (topicList == null) {
        topicList = new ArrayList<Map<String, Object>>();
        eachCluster.put(TOPICSTATS, topicList);
      }
      eachCluster.put(CLUSTERLATENCY, convertTLatencyListOfMaps
          (clusterAggPercentileMap.get(new Tuple(null, tuple.getTier(),
              tuple.getCluster(), tuple.getTimestamp(), null)).entrySet()));
      Map<String, Object> topicStat = new HashMap<String, Object>();
      topicStat.put(TOPIC, tuple.getTopic());
      topicStat.put(RECEIVED, tuple.getReceived());
      topicStat.put(SENT, tuple.getSent());
      topicStat.put(TOPICLATENCY, convertTLatencyListOfMaps(percentileMap.get
          (tuple).entrySet()));
      topicList.add(topicStat);
    }
    return map;
  }
  
  private static List<Map<String, String>> convertTLatencyListOfMaps(
      Set<Entry<Float, Integer>> latencyMap) {
    List<Map<String, String>> returnList =
        new LinkedList<Map<String, String>>();
    for (Entry<Float, Integer> eachEntry : latencyMap) {
      Map<String, String> eachMap = new HashMap<String, String>();
      eachMap.put(PERCENTILE, eachEntry.getKey().toString());
      eachMap.put(LATENCY, eachEntry.getValue().toString());
      returnList.add(eachMap);
    }
    return returnList;
  }


  @SuppressWarnings("unchecked")
  private static Map<String, Object> covertToUIFriendlyObject(AuditDbQuery query) {
    Map<String, Object> returnMap = new HashMap<String, Object>();
    Map<String, Map<String, Map<String, Object>>> map = convertToMap(query);
    List<Object> datapoints = new ArrayList<Object>();
    for (String eachTier : map.keySet()) {
      Map<String, Object> eachTierMap = new HashMap<String, Object>();
      eachTierMap.put(TIER, eachTier);
      List<Map<String, Object>> modifiedtimeserierList = new ArrayList<Map<String, Object>>();
      eachTierMap.put(TIERWISEPOINTLIST, modifiedtimeserierList);
      for (String eachTimeKey : map.get(eachTier).keySet()) {
        Object eachTimeData = map.get(eachTier).get(eachTimeKey);
        Map<String, Object> eachObject = (Map<String, Object>) eachTimeData;
        List<Object> clusterwiseMap = new ArrayList<Object>();
        Map<String, Object> changedMap = new HashMap<String, Object>();
        Set<String> keys = eachObject.keySet();
        for (String eachKey : keys) {
          if (!eachKey.equals(AGGRECEIVED) && !eachKey.equals(AGGSENT) &&
              !eachKey.equals(OVERALLLATENCY)) {
            clusterwiseMap.add(eachObject.get(eachKey));
          } else {
            changedMap.put(eachKey, eachObject.get(eachKey));
          }
        }
        changedMap.put(TIME, eachTimeKey);
        changedMap.put(CLUSTERCOUNTLIST, clusterwiseMap);
        modifiedtimeserierList.add(changedMap);
      }
      datapoints.add(eachTierMap);
    }
    returnMap.put(DATAPOINTS, datapoints);
    return returnMap;
  }

  public static String convertToJson(AuditDbQuery query) {
    JSONObject newObject = new JSONObject(covertToUIFriendlyObject(query));
    return newObject.toString();
  }
  
  private static Set<Tuple> groupTuples(final Set<Tuple> grouped,
                                             GroupBy groupBy) {
    Map<GroupBy.Group, Map<LatencyColumns, Long>> unGroupedMap =
        new HashMap<GroupBy.Group, Map<LatencyColumns, Long>>();
    Map<GroupBy.Group, Tuple> tupleMap = new HashMap<GroupBy.Group, Tuple>();
    for (final Tuple eachTuple : grouped) {
      GroupBy.Group group = groupBy.getGroup(eachTuple.getTupleKey());
      Map<LatencyColumns, Long> latencyMap = unGroupedMap.get(group);
      Tuple aggregatedTuple = tupleMap.get(group);
      if (latencyMap == null) {
        latencyMap = new HashMap<LatencyColumns, Long>();
        Map<LatencyColumns, Long> eachTupleMap = eachTuple.getLatencyCountMap();
        if (groupBy.getGroupByColumns().contains(Column.CLUSTER)) {
          aggregatedTuple = new Tuple(null, eachTuple.getTier(),
              eachTuple.getCluster(), eachTuple.getTimestamp(), null, null,
              eachTuple.getSent());
        } else {
          aggregatedTuple = new Tuple(null, eachTuple.getTier(), null,
              eachTuple.getTimestamp(), null, null, eachTuple.getSent());
        }
        unGroupedMap.put(group, latencyMap);
        tupleMap.put(group, aggregatedTuple);
        for (LatencyColumns latencyColumn : LatencyColumns.values()) {
          latencyMap.put(latencyColumn, eachTupleMap.get(latencyColumn));
        }
      } else {
        Map<LatencyColumns, Long> eachTupleMap = eachTuple.getLatencyCountMap();
        for (LatencyColumns latencyColumn : LatencyColumns.values()) {
          latencyMap.put(latencyColumn, latencyMap.get(latencyColumn) +
              eachTupleMap.get(latencyColumn));
        }
        aggregatedTuple
            .setSent(aggregatedTuple.getSent() + eachTuple.getSent());
      }
    }
    Set<Tuple> returnSet = new HashSet<Tuple>();
    for (GroupBy.Group eachKey : unGroupedMap.keySet()) {
      Tuple eachTuple = tupleMap.get(eachKey);
      eachTuple.setLatencyCountMap(unGroupedMap.get(eachKey));
      returnSet.add(eachTuple);
    }
    return returnSet;
  }
  
}
