package com.inmobi.conduit.visualization.client;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface DataServiceAsync {
  public void getStreamAndClusterList(AsyncCallback<String> callback);

  public void getTopologyData(String filterValues, AsyncCallback<String> callback);

  public void getTierLatencyData(String filterValues, AsyncCallback<String> callback);

  public void getTimeLineData(String filterValues, AsyncCallback<String> callback);
}
