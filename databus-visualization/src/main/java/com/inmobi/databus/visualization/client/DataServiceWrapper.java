package com.inmobi.databus.visualization.client;

import com.google.gwt.core.client.GWT;
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

  public void getData(String clientJson, final AsyncCallback<String> callback) {
    service.getData(clientJson, new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        callback.onFailure(caught);
      }

      public void onSuccess(String result) {
        callback.onSuccess(result);
      }
    });
  }

  public void getStreamList(final AsyncCallback<String> callback) {
    service.getStreamList(new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        callback.onFailure(caught);
      }

      public void onSuccess(String result) {
        callback.onSuccess(result);
      }
    });
  }

  public void getClusterList(final AsyncCallback<String> callback) {
    service.getClusterList(new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        callback.onFailure(caught);
      }

      public void onSuccess(String result) {
        callback.onSuccess(result);
      }
    });
  }
}
