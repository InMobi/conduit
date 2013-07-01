package com.inmobi.databus.visualization.client;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface DataServiceAsync {
  public void getStreamList(AsyncCallback<String> callback);

  public void getClusterList(AsyncCallback<String> callback);

  public void getData(String filterValues, AsyncCallback<String> callback);
}
