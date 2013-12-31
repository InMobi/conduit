package com.inmobi.conduit.visualization.client;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface DataServiceAsync {
  public void getStreamAndClusterList(AsyncCallback<String> callback);

  public void getData(String filterValues, AsyncCallback<String> callback);

  public void getTimeLineData(String filterValues, AsyncCallback<String> callback);
}
