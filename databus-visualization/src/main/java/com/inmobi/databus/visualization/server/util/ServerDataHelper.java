package com.inmobi.databus.visualization.server.util;

import com.google.protobuf.gwt.server.ServerJsonStreamFactory;
import com.inmobi.databus.visualization.server.MessageStats;
import com.inmobi.databus.visualization.server.Node;
import com.inmobi.databus.visualization.server.VisualizationProperties;
import com.inmobi.databus.visualization.shared.RequestResponse;
import com.inmobi.messaging.consumer.audit.Tier;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
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

  public String setStreamListResponse(List<String> streams) {
    RequestResponse.StreamListResponse streamListResponse =
        RequestResponse.StreamListResponse.newBuilder().addAllStream(streams)
            .build();
    return ServerJsonStreamFactory.getInstance().serializeMessage(
        RequestResponse.Response.newBuilder()
            .setStreamListResponse(streamListResponse).build());
  }

  public String setColoListResponse(List<String> colos) {
    RequestResponse.ColoListResponse coloListResponse =
        RequestResponse.ColoListResponse.newBuilder().addAllColo(colos).build();
    return ServerJsonStreamFactory.getInstance().serializeMessage(
        RequestResponse.Response.newBuilder()
            .setColoListResponse(coloListResponse).build());
  }

  public String setGraphDataResponse(List<Node> nodeList) {
    JSONObject newObject = new JSONObject();
    JSONArray nodeArray = new JSONArray();
    try {
      LOG.debug("Parsing the nodeList into a JSON");
      for (Node node : nodeList) {
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
    RequestResponse.GraphDataResponse response =
        RequestResponse.GraphDataResponse.newBuilder()
            .setJsonString(newObject.toString()).setAgentSla(Integer.parseInt(
            VisualizationProperties
                .get(VisualizationProperties.PropNames.AGENT_SLA))).setVipSla(
            Integer.parseInt(VisualizationProperties
                .get(VisualizationProperties.PropNames.VIP_SLA)))
            .setCollectorSla(Integer.parseInt(VisualizationProperties
                .get(VisualizationProperties.PropNames.COLLECTOR_SLA)))
            .setHdfsSla(Integer.parseInt(VisualizationProperties
                .get(VisualizationProperties.PropNames.HDFS_SLA))).setLocalSla(
            Integer.parseInt(VisualizationProperties
                .get(VisualizationProperties.PropNames.LOCAL_SLA))).setMergeSla(
            Integer.parseInt(VisualizationProperties
                .get(VisualizationProperties.PropNames.MERGE_SLA)))
            .setMirrorSla(Integer.parseInt(VisualizationProperties
                .get(VisualizationProperties.PropNames.MIRROR_SLA)))
            .setPercentileForSla(Float.parseFloat(VisualizationProperties
                .get(VisualizationProperties.PropNames.PERCENTILE_FOR_SLA)))
            .setPercentageForLoss(Float.parseFloat(VisualizationProperties
                .get(VisualizationProperties.PropNames.PERCENTAGE_FOR_LOSS))).build();
    return ServerJsonStreamFactory.getInstance().serializeMessage(
        RequestResponse.Response.newBuilder().setGraphDataResponse(response)
            .build());
  }
}
