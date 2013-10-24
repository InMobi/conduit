package com.inmobi.databus.visualization.server.util;

import com.google.protobuf.gwt.server.ServerJsonStreamFactory;
import com.inmobi.databus.audit.Tuple;
import com.inmobi.databus.visualization.server.*;
import com.inmobi.databus.visualization.shared.RequestResponse;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

  public String setGraphDataResponse(Map<NodeKey, Node> nodeMap,
                                     Map<Tuple, Map<Float, Integer>> tierLatencyMap,
                                     VisualizationProperties properties) {
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
    RequestResponse.TierLatencyResponse tierLatency =
        setTierLatencyResponseObject(tierLatencyMap, properties);
    return ServerJsonStreamFactory.getInstance().serializeMessage(
        RequestResponse.Response.newBuilder().setGraphDataResponse(
            RequestResponse.GraphDataResponse.newBuilder()
                .setJsonString(newObject.toString())
                .setTierLatencyResponse(tierLatency)).build());
  }

  private RequestResponse.TierLatencyResponse setTierLatencyResponseObject(
      Map<Tuple, Map<Float, Integer>> tierLatencyMap, VisualizationProperties
      properties) {
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
    return RequestResponse.TierLatencyResponse.newBuilder().addAllTierLatencyObjList(
        tierLatencyObjList).build();
  }

  public String setLoadMainPanelResponse(List<String> streamList,
                                         List<String> clusterList,
                                         VisualizationProperties properties) {
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
            .setMirrorSla(properties.get(ServerConstants.MIRROR_SLA)).build();
    RequestResponse.LoadMainPanelResponse loadMainPanelResponse =
        RequestResponse.LoadMainPanelResponse.newBuilder()
            .addAllStream(streamList).addAllCluster(clusterList)
            .setClientConfig(clientConfiguration).build();
    return ServerJsonStreamFactory.getInstance().serializeMessage(
        RequestResponse.Response.newBuilder()
            .setLoadMainPanelResponse(loadMainPanelResponse).build());
  }
}
