package com.inmobi.conduit.visualization.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.http.client.Request;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.ServiceDefTarget;

public class DataServiceWrapper {
  private final DataServiceAsync service =
      (DataServiceAsync) GWT.create(DataService.class);
  private final String moduleRelativeURL = GWT.getModuleBaseURL() + "graph";
  private final ServiceDefTarget target = (ServiceDefTarget) service;

  public DataServiceWrapper() {
    target.setServiceEntryPoint(moduleRelativeURL);
  }

  public Request getTopologyData(String clientJson, final AsyncCallback<String>
      callback) {
    Request request = service.getTopologyData(clientJson, new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        callback.onFailure(caught);
      }

      public void onSuccess(String result) {
        callback.onSuccess(result);
      }
    });
    return request;
  }

  public Request getTierLatencyData(String clientJson, final AsyncCallback<String>
      callback) {
    Request request = service.getTierLatencyData(clientJson, new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        callback.onFailure(caught);
      }

      public void onSuccess(String result) {
        callback.onSuccess(result);
      }
    });
    return request;
  }

  public void getStreamAndClusterList(final AsyncCallback<String> callback) {
    service.getStreamAndClusterList(new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        callback.onFailure(caught);
      }

      public void onSuccess(String result) {
        callback.onSuccess(result);
      }
    });
  }

  public Request getTimeLineData(String clientJson, final AsyncCallback<String>
      callback) {
    Request request = service.getTimeLineData(clientJson, new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        callback.onFailure(caught);
      }

      public void onSuccess(String result) {
        callback.onSuccess(result);
      }
    });
    return request;
  }
}
