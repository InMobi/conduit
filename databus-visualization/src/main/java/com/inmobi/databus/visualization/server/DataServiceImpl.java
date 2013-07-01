package com.inmobi.databus.visualization.server;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import com.inmobi.databus.visualization.client.DataService;

/**
 * The server side implementation of the RPC service.
 */
public class DataServiceImpl extends RemoteServiceServlet
    implements DataService {
  private static final long serialVersionUID = 1L;
  DataServiceManager serviceManager = DataServiceManager.get();


  public String getData(String filterValues) {
    return serviceManager.getData(filterValues);
  }

  public String getStreamList() {
    return serviceManager.getStreamList();
  }

  public String getClusterList() {
    return serviceManager.getClusterList();
  }

}
