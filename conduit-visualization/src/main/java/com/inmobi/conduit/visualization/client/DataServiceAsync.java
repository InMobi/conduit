package com.inmobi.conduit.visualization.client;

import com.google.gwt.http.client.Request;
import com.google.gwt.user.client.rpc.AsyncCallback;

public interface DataServiceAsync {
  public void getStreamAndClusterList(AsyncCallback<String> callback);

  public Request getTopologyData(String filterValues, AsyncCallback<String>
      callback);

  public Request getTierLatencyData(String filterValues, AsyncCallback<String>
      callback);

  public Request getTimeLineData(String filterValues, AsyncCallback<String>
      callback);
}
