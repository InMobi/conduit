package com.inmobi.databus.visualization.client.util;

import com.google.protobuf.gwt.client.ClientJsonStreamFactory;
import com.inmobi.databus.visualization.client.ClientConstants;
import com.inmobi.databus.visualization.shared.RequestResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientDataHelper {
  private static ClientDataHelper dataHelperInstance;

  public static ClientDataHelper getInstance() {
    if (dataHelperInstance == null) {
      dataHelperInstance = new ClientDataHelper();
    }
    return dataHelperInstance;
  }

  public String setGraphDataRequest(String startTime, String endTime,
                                    String stream, String colo) {
    RequestResponse.Request request;
    request = RequestResponse.Request.newBuilder().setGraphDataRequest(
        RequestResponse.GraphDataRequest.newBuilder().setStartTime(startTime)
            .setEndTime(endTime).setStream(stream).setColo(colo).build())
        .build();
    return ClientJsonStreamFactory.getInstance().serializeMessage(request);
  }

  public List<String> getStreamsListResponse(String serverJson) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getStreamListResponse().getStreamList();
  }

  public List<String> getClusterListResponse(String serverJson) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getColoListResponse().getColoList();
  }

  public String getJsonStrongFromGraphDataResponse(String serverJson) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getGraphDataResponse().getJsonString();
  }

  public Map<String, Integer> getSlaMapFromGraphDataResponse(
      String serverJson) {
    RequestResponse.Response response = null;
    Map<String, Integer> slaMap = new HashMap<String, Integer>();
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    slaMap.put(ClientConstants.AGENT, response.getGraphDataResponse()
        .getAgentSla());
    slaMap.put(ClientConstants.VIP, response.getGraphDataResponse()
        .getVipSla());
    slaMap.put(ClientConstants.COLLECTOR, response.getGraphDataResponse()
        .getCollectorSla());
    slaMap.put(ClientConstants.HDFS, response.getGraphDataResponse()
        .getHdfsSla());
    slaMap.put(ClientConstants.LOCAL, response.getGraphDataResponse()
        .getLocalSla());
    slaMap.put(ClientConstants.MERGE, response.getGraphDataResponse()
        .getMergeSla());
    slaMap.put(ClientConstants.MIRROR, response.getGraphDataResponse()
        .getMirrorSla());
    return slaMap;
  }

  public Float getPercentileForSlaFromGraphDataResponse(String serverJson) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getGraphDataResponse().getPercentileForSla();
  }

  public Float getPercentageForLossFromGraphDataResponse(String serverJson) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getGraphDataResponse().getPercentageForLoss();
  }
}
