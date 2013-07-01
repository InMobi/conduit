package com.inmobi.databus.visualization.client;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("graph")
public interface DataService extends RemoteService {
  public String getStreamList();

  public String getClusterList();

  public String getData(String filterValues);
}
