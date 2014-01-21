package com.inmobi.conduit.visualization.server;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import com.inmobi.conduit.visualization.client.DataService;

/**
 * The server side implementation of the RPC service.
 */
public class DataServiceImpl extends RemoteServiceServlet
    implements DataService {
  private static final long serialVersionUID = 1L;
  DataServiceManager serviceManager = DataServiceManager.get(true);


  public String getData(String filterValues) {
    return serviceManager.getData(filterValues);
  }

  public String getStreamAndClusterList() {
    return serviceManager.getStreamAndClusterList();
  }

  public String getTimeLineData(String filterValues) {
    return serviceManager.getTimeLineData(filterValues);
  }

}
