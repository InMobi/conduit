package com.inmobi.databus.visualization.server.util;

import com.google.protobuf.gwt.server.ServerJsonStreamFactory;
import com.inmobi.databus.visualization.shared.RequestResponse;

import java.io.IOException;
import java.util.List;

public class TestUtil {

  public static List<String> getStreamListFromResponse(String response)
      throws IOException {
    return RequestResponse.Response.newBuilder().readFrom(
        ServerJsonStreamFactory.getInstance()
            .createNewStreamFromJson(response)).build()
        .getLoadMainPanelResponse().getStreamList();
  }

  public static List<String> getClusterListFromResponse(String response)
      throws IOException {
    return RequestResponse.Response.newBuilder().readFrom(
        ServerJsonStreamFactory.getInstance()
            .createNewStreamFromJson(response)).build()
        .getLoadMainPanelResponse().getClusterList();
  }
}
