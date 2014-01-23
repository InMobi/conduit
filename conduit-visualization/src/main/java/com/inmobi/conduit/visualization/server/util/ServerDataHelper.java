package com.inmobi.conduit.visualization.server.util;

import com.google.protobuf.gwt.server.ServerJsonStreamFactory;
import com.inmobi.conduit.visualization.server.MessageStats;
import com.inmobi.conduit.visualization.server.Node;
import com.inmobi.conduit.visualization.server.NodeKey;
import com.inmobi.conduit.visualization.server.ServerConstants;
import com.inmobi.conduit.visualization.server.VisualizationProperties;
import com.inmobi.conduit.visualization.shared.RequestResponse;
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

  public String setTopologyDataResponse(Map<NodeKey, Node> nodeMap) {
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

  public static String setTimeLineDataResponse(String jsonResult) {
    return ServerJsonStreamFactory.getInstance().serializeMessage(
        RequestResponse.Response
            .newBuilder()
            .setTimeLineGraphResponse(
                RequestResponse.TimeLineGraphResponse.newBuilder()
                    .setJsonString(jsonResult)).build());
  }
  
  
  
  @SuppressWarnings("unchecked")
  private static Map<String, Map<String, Map<String, Object>>> convertToMap(AuditDbQuery query, AuditDbQuery aggregatedQuery) {
    Map<String, Map<String, Map<String, Object>>> map =
        new HashMap<String, Map<String, Map<String, Object>>>();

    for (Tuple eachTuple : query.getTupleSet()) {
      Map<String, Map<String, Object>> eachTierMap =
          map.get(eachTuple.getTier());
      if (eachTierMap == null) {
        eachTierMap = new HashMap<String, Map<String, Object>>();
        map.put(eachTuple.getTier(), eachTierMap);
      }
      Map<String, Object> eachTSMap =
          eachTierMap.get("" + eachTuple.getTimestamp().getTime());
      if (eachTSMap == null) {
        eachTSMap = new TreeMap<String, Object>();
        eachTierMap.put("" + eachTuple.getTimestamp().getTime(), eachTSMap);
        eachTSMap.put("aggreceived", eachTuple.getReceived());
        eachTSMap.put("aggsent", eachTuple.getSent());
        eachTSMap.put("overallLatency" , convertTLatencyListOfMaps(aggregatedQuery.getPercentile().get(new Tuple(eachTuple.getHostname(), 
            eachTuple.getTier(), eachTuple.getCluster(), eachTuple.getTimestamp(), null)).entrySet()));
        System.out.println();
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
      clusterStat.put("latencyList", convertTLatencyListOfMaps(query.getPercentile().get(eachTuple).entrySet()));
      clusterList.add(clusterStat);

    }
    return map;

  }
  
  private static List<Map<String, String>> convertTLatencyListOfMaps(
      Set<Entry<Float, Integer>> latencyMap) {
    List<Map<String, String>> returnList =
        new LinkedList<Map<String, String>>();
    for (Entry<Float, Integer> eachEntry : latencyMap) {
      Map<String, String> eachMap = new HashMap<String, String>();
      eachMap.put("percentile", eachEntry.getKey().toString());
      eachMap.put("latency", eachEntry.getValue().toString());
      returnList.add(eachMap);
    }
    return returnList;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> covertToUIFriendlyObject(AuditDbQuery query, AuditDbQuery aggregatedQuery) {
    Map<String, Object> returnMap = new HashMap<String, Object>();
    Map<String, Map<String, Map<String, Object>>> map = convertToMap(query ,aggregatedQuery);

    List<Object> datapoints = new ArrayList<Object>();
    for (String eachTier : map.keySet()) {
      Map<String, Object> eachTierMap = new HashMap<String, Object>();
      eachTierMap.put("tier", eachTier);
      Set<Map<String, Object>> modifiedtimeserierList = new HashSet<Map<String, Object>>();
      eachTierMap.put("tierWisePointList", modifiedtimeserierList);
      for (String eachTimeKey : map.get(eachTier).keySet()) {
        Object eachTimeData = map.get(eachTier).get(eachTimeKey);
        Map<String, Object> eachObject = (Map<String, Object>) eachTimeData;
        List<Object> steamwiseMap = new ArrayList<Object>();
        Map<String, Object> changedMap = new HashMap<String, Object>();
        Set<String> keys = eachObject.keySet();
        for (String eachKey : keys) {
          if (!eachKey.equals("aggreceived") && !eachKey.equals("aggsent") &&!eachKey.equals("overallLatency")) {

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

  public static String convertToJson(AuditDbQuery query, AuditDbQuery aggregatedQuery) {
    JSONObject newObject = new JSONObject(covertToUIFriendlyObject(query , aggregatedQuery));
    return newObject.toString();

  }
}
