package com.inmobi.conduit.visualization.server.util;

import com.google.protobuf.gwt.server.ServerJsonStreamFactory;
import com.inmobi.conduit.audit.*;
import com.inmobi.conduit.audit.query.AuditDbQuery;
import com.inmobi.conduit.audit.util.AuditDBHelper;
import com.inmobi.conduit.audit.util.TimeLineAuditDBHelper;
import com.inmobi.conduit.visualization.server.*;
import com.inmobi.conduit.visualization.shared.RequestResponse;
import com.inmobi.messaging.ClientConfig;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

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

  public String setTopologyDataResponse(Map<NodeKey, Node> nodeMap) {
    JSONObject newObject = new JSONObject();
    JSONArray nodeArray = new JSONArray();
    try {
      LOG.debug("Parsing the nodeList into a JSON");
      for (Node node : nodeMap.values()) {
        LOG.debug("Parsing node : " + node.toString());
        JSONObject nodeObject = new JSONObject();
        nodeObject.put(JSONKeyConstants.NAME, node.getName());
        nodeObject.put(JSONKeyConstants.CLUSTER, node.getClusterName());
        nodeObject.put(JSONKeyConstants.TIER, node.getTier());
        nodeObject.put(JSONKeyConstants.AGGREGATERECEIVED,
            String.valueOf(node.getAggregateMessagesReceived()));
        JSONArray topicStatsArray = new JSONArray();
        if (node.getReceivedMessagesList() != null) {
          for (MessageStats messagestat : node.getReceivedMessagesList()) {
            JSONObject topicStatsObject = new JSONObject();
            topicStatsObject.put(JSONKeyConstants.TOPIC, messagestat.getTopic());
            topicStatsObject.put(JSONKeyConstants.MESSAGES, messagestat.getMessages
                ());
            topicStatsObject.put(JSONKeyConstants.HOSTNAME, messagestat.getHostname
                ());
            topicStatsArray.put(topicStatsObject);
          }
        }
        nodeObject.put(JSONKeyConstants.RECEIVEDTOPICSTATSLIST, topicStatsArray);
        nodeObject.put(JSONKeyConstants.AGGREGATESENT,
            String.valueOf(node.getAggregateMessagesSent()));
        JSONArray senttopicStatsArray = new JSONArray();
        if (node.getSentMessagesList() != null) {
          for (MessageStats messagestat : node.getSentMessagesList()) {
            JSONObject topicStatsObject = new JSONObject();
            topicStatsObject.put(JSONKeyConstants.TOPIC, messagestat.getTopic());
            topicStatsObject.put(JSONKeyConstants.MESSAGES, messagestat.getMessages
                ());
            topicStatsObject.put(JSONKeyConstants.HOSTNAME, messagestat.getHostname
                ());
            senttopicStatsArray.put(topicStatsObject);
          }
        }
        nodeObject.put(JSONKeyConstants.SENTTOPICSTATSLIST, senttopicStatsArray);
        if ((node.getTier().equalsIgnoreCase(Tier.MERGE.toString()) ||
            node.getTier().equalsIgnoreCase(Tier.MIRROR.toString())) &&
            node.getSourceList() != null) {

          JSONArray steamSourceArray = new JSONArray();
          for (Map.Entry<String, Set<String>> sourceEntry: node
              .getTopicSourceList().entrySet()) {
            JSONObject object = new JSONObject();
            object.put(JSONKeyConstants.TOPIC, sourceEntry.getKey());
            object.put(JSONKeyConstants.SOURCELIST, sourceEntry.getValue());
            steamSourceArray.put(object);
          }
          nodeObject.put(JSONKeyConstants.TOPICSOURCE, steamSourceArray);
          JSONArray sourceListArray = new JSONArray();
          sourceListArray.put(node.getSourceList());
          nodeObject.put(JSONKeyConstants.SOURCE, sourceListArray);
        }
        JSONArray percentileArray = new JSONArray();
        for(Map.Entry<Float, Integer> entry : node.getPercentileMap()
            .entrySet()) {
          JSONObject percentileObject = new JSONObject();
          percentileObject.put(JSONKeyConstants.PERCENTILE, entry.getKey());
          percentileObject.put(JSONKeyConstants.LATENCY, entry.getValue());
          percentileArray.put(percentileObject);
        }
        nodeObject.put(JSONKeyConstants.OVERALLLATENCY, percentileArray);
        JSONArray topicPercentileArray = new JSONArray();
        for(Map.Entry<String, Map<Float, Integer>> entry : node.getPerTopicPercentileMap().entrySet()) {
          JSONObject topicPercentileObject = new JSONObject();
          JSONArray topicLatency = new JSONArray();
          for (Map.Entry<Float, Integer> percentileEntry : entry.getValue()
              .entrySet()) {
            JSONObject percentileObject = new JSONObject();
            percentileObject.put(JSONKeyConstants.PERCENTILE, percentileEntry.getKey());
            percentileObject.put(JSONKeyConstants.LATENCY, percentileEntry.getValue
                ());
            topicLatency.put(percentileObject);
          }
          topicPercentileObject.put(JSONKeyConstants.TOPIC, entry.getKey());
          topicPercentileObject.put(JSONKeyConstants.PERCENTILELATENCYLIST, topicLatency);
          topicPercentileArray.put(topicPercentileObject);
        }
        nodeObject.put(JSONKeyConstants.TOPICLATENCY, topicPercentileArray);
        LOG.debug("Parsed node JSON : " + nodeObject.toString());
        nodeArray.put(nodeObject);
      }
      newObject.put(JSONKeyConstants.NODES, nodeArray);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ServerJsonStreamFactory.getInstance().serializeMessage(
        RequestResponse.Response.newBuilder().setTopologyDataResponse(
            RequestResponse.TopologyDataResponse.newBuilder()
                .setJsonString(newObject.toString())).build());
  }

  public String setTierLatencyResponseObject(Map<Tuple, Map<Float,
      Integer>> tierLatencyMap,VisualizationProperties properties) {
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
                .addAllTierLatencyObjList(tierLatencyObjList)).build());
  }

  public String setLoadMainPanelResponse(List<String> streamList,
                                         List<String> clusterList,
                                         VisualizationProperties properties,
                                         ClientConfig feederConfig) {
    int rolledUpTillDays = feederConfig.getInteger(ServerConstants
        .ROLLEDUP_TILL_DAYS, ServerConstants.DEFAULT_HOURLY_ROLLUP_TILLDAYS);
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
            .setRolleduptilldays(rolledUpTillDays)
            .setDailyRolledupTilldays(feederConfig.getInteger(
                ServerConstants.DAILY_ROLLEDUP_TILL_DAYS,
                ServerConstants.DEFAULT_GAP_BTW_ROLLUP_TILLDAYS + rolledUpTillDays))
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
                                               TimeLineAuditDBHelper helper) {
    return ServerJsonStreamFactory.getInstance().serializeMessage
        (RequestResponse.Response.newBuilder().setTimeLineGraphResponse
            (RequestResponse.TimeLineGraphResponse.newBuilder().setJsonString
                (jsonResult).setTimebucket(helper.getTimeGroup())).build());
  }

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
        eachTSMap.put(JSONKeyConstants.AGGRECEIVED, tuple.getReceived());
        eachTSMap.put(JSONKeyConstants.AGGSENT, tuple.getSent());
        eachTSMap.put(JSONKeyConstants.OVERALLLATENCY,
            convertTLatencyListOfMaps
            (allAggPercentileMap.get(new Tuple(tuple.getHostname(),
                tuple.getTier(), null, tuple.getTimestamp(), null)).entrySet()));
      } else {
        eachTSMap.put(JSONKeyConstants.AGGRECEIVED,
            (Long) eachTSMap.get(JSONKeyConstants.AGGRECEIVED) + tuple
                .getReceived());
        eachTSMap.put(JSONKeyConstants.AGGSENT, (Long) eachTSMap.get
            (JSONKeyConstants.AGGSENT) + tuple.getSent());
      }
      Map<String, Object> eachCluster = (Map<String, Object>) eachTSMap.get
          (tuple.getCluster());
      if (eachCluster == null) {
        eachCluster = new HashMap<String, Object>();
        eachTSMap.put(tuple.getCluster(), eachCluster);
        eachCluster.put(JSONKeyConstants.CLUSTER, tuple.getCluster());
      }
      List<Map<String, Object>> topicList =
          (List<Map<String, Object>>) eachCluster.get(JSONKeyConstants
              .TOPICSTATS);
      if (topicList == null) {
        topicList = new ArrayList<Map<String, Object>>();
        eachCluster.put(JSONKeyConstants.TOPICSTATS, topicList);
      }
      eachCluster.put(JSONKeyConstants.CLUSTERLATENCY,
          convertTLatencyListOfMaps(clusterAggPercentileMap.get(new Tuple
              (null, tuple.getTier(), tuple.getCluster(),
                  tuple.getTimestamp(), null)).entrySet()));
      Map<String, Object> topicStat = new HashMap<String, Object>();
      topicStat.put(JSONKeyConstants.TOPIC, tuple.getTopic());
      topicStat.put(JSONKeyConstants.RECEIVED, tuple.getReceived());
      topicStat.put(JSONKeyConstants.SENT, tuple.getSent());
      topicStat.put(JSONKeyConstants.TOPICLATENCY, convertTLatencyListOfMaps
          (percentileMap.get(tuple).entrySet()));
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
      eachMap.put(JSONKeyConstants.PERCENTILE, eachEntry.getKey().toString());
      eachMap.put(JSONKeyConstants.LATENCY, eachEntry.getValue().toString());
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
      eachTierMap.put(JSONKeyConstants.TIER, eachTier);
      List<Map<String, Object>> modifiedtimeserierList = new ArrayList<Map<String, Object>>();
      eachTierMap.put(JSONKeyConstants.TIERWISEPOINTLIST,
          modifiedtimeserierList);
      for (String eachTimeKey : map.get(eachTier).keySet()) {
        Object eachTimeData = map.get(eachTier).get(eachTimeKey);
        Map<String, Object> eachObject = (Map<String, Object>) eachTimeData;
        List<Object> clusterwiseMap = new ArrayList<Object>();
        Map<String, Object> changedMap = new HashMap<String, Object>();
        Set<String> keys = eachObject.keySet();
        for (String eachKey : keys) {
          if (!eachKey.equals(JSONKeyConstants.AGGRECEIVED) && !eachKey
              .equals(JSONKeyConstants.AGGSENT) &&
              !eachKey.equals(JSONKeyConstants.OVERALLLATENCY)) {
            clusterwiseMap.add(eachObject.get(eachKey));
          } else {
            changedMap.put(eachKey, eachObject.get(eachKey));
          }
        }
        changedMap.put(JSONKeyConstants.TIME, eachTimeKey);
        changedMap.put(JSONKeyConstants.CLUSTERCOUNTLIST, clusterwiseMap);
        modifiedtimeserierList.add(changedMap);
      }
      datapoints.add(eachTierMap);
    }
    returnMap.put(JSONKeyConstants.DATAPOINTS, datapoints);
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
