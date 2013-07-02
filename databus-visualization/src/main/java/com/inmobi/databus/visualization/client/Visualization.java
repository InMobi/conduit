package com.inmobi.databus.visualization.client;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.google.gwt.user.datepicker.client.DatePicker;
import com.inmobi.databus.visualization.client.util.ClientDataHelper;
import com.inmobi.databus.visualization.client.util.DateUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Visualization implements EntryPoint, ClickHandler {

  private HorizontalPanel header, filterPanel;
  private ListBox streamsList, clusterList;
  private TextBox startTime, endtime;
  private VerticalPanel startTimeVPanel, endTimeVPanel, streamVPanel,
      clusterVPanel;
  private ListBox stTimeHour, stTimeMinute, edTimeHour, edTimeMinute;
  private DatePicker stDatePicker, endDatePicker;
  private HorizontalPanel etTimeHPanel, stTimeHPanel;
  private Label stTimeLabel, etTimeLabel, streamLabel, clusterLabel;
  private PopupPanel stcalendarPopup, etcalendarPopup;

  List<String> streams = new ArrayList<String>(), clusters =
      new ArrayList<String>();
  private String stTime, edTime;
  private boolean isReloaded = false;
  DataServiceWrapper serviceInstance = new DataServiceWrapper();

  public void onModuleLoad() {
    if (isReloaded) {
      loadMainPanel();
    } else {
      buildStreamsList();
    }
  }

  private void buildStreamsList() {
    System.out.println("Building stream list...");
    serviceInstance.getStreamList(new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        caught.printStackTrace();
      }

      public void onSuccess(String result) {
        System.out.println("Completed building stream list.");
        streams.addAll(
            ClientDataHelper.getInstance().getStreamsListResponse(result));
        buildClusterList();
      }
    });
  }

  private void buildClusterList() {
    System.out.println("Building cluster list...");
    serviceInstance.getClusterList(new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        caught.printStackTrace();
      }

      public void onSuccess(String result) {
        clusters
            .addAll(ClientDataHelper.getInstance().getClusterListResponse(result));
        loadMainPanel();
      }
    });
  }

  private void loadMainPanel() {
    System.out.println("Loading main panel...");
    loadHeader();
    loadFilterPanel();
    RootPanel.get("headerContainer").add(header);
    RootPanel.get("filterContainer").add(filterPanel);
    System.out.println("Loaded main panel");

    String startTime = Window.Location.getParameter(ClientConstants.QUERY_FROM_TIME);
    String endTime = Window.Location.getParameter(ClientConstants.QUERY_TO_TIME);
    String cluster = Window.Location.getParameter(ClientConstants.QUERY_CLUSTER);
    String stream = Window.Location.getParameter(ClientConstants.QUERY_STREAM);
    String drillDownCluster = Window.Location.getParameter(ClientConstants.GRAPH_CLUSTER);
    String drillDownStream = Window.Location.getParameter(ClientConstants.GRAPH_STREAM);
    String nodesJson = null;
    if (startTime != null && endTime != null && cluster != null &&
        stream != null) {
      System.out.println("Retrieving parameters from URL");
      setSelectedParameterValues(startTime, endTime, cluster, stream);
      sendRequest(startTime, endTime, cluster, stream, nodesJson,
          drillDownCluster, drillDownStream);
    }
  }

  private void setSelectedParameterValues(String stTime, String endTime,
                                          String cluster, String stream) {
    startTime.setText(DateUtils.getTextBoxValueFromDateString(stTime));
    endtime.setText(DateUtils.getTextBoxValueFromDateString(endTime));
    stDatePicker.setValue(DateUtils.getDateFromDateString(stTime));
    endDatePicker.setValue(DateUtils.getDateFromDateString(endTime));
    setSelectedInListBox(stTimeHour, DateUtils.getHourFromDateString(stTime));
    setSelectedInListBox(stTimeMinute,
        DateUtils.getMinuteFromDateString(stTime));
    setSelectedInListBox(edTimeHour, DateUtils.getHourFromDateString(endTime));
    setSelectedInListBox(edTimeMinute,
        DateUtils.getMinuteFromDateString(endTime));
    setSelectedInListBox(clusterList, cluster);
    setSelectedInListBox(streamsList, stream);
  }

  private void setSelectedInListBox(ListBox listBox, String selectedString) {
    int selectedIndex = 0;
    for (int i = 0; i < listBox.getItemCount(); i++) {
      if (listBox.getItemText(i).equals(selectedString)) {
        selectedIndex = i;
        break;
      }
    }
    listBox.setSelectedIndex(selectedIndex);
  }

  private void loadHeader() {
    header = new HorizontalPanel();
    HTML heading = new HTML("<h1>Databus Visualization</h1>");
    heading.getElement().setId("heading");
    header.getElement().setId("header");
    header.add(heading);
  }

  private void loadFilterPanel() {
    stcalendarPopup = new PopupPanel(true);
    etcalendarPopup = new PopupPanel(true);
    filterPanel = new HorizontalPanel();
    startTime = new TextBox();
    endtime = new TextBox();
    streamsList = new ListBox();
    clusterList = new ListBox();
    stDatePicker = new DatePicker();
    endDatePicker = new DatePicker();
    stTimeHour = new ListBox();
    stTimeMinute = new ListBox();
    edTimeHour = new ListBox();
    edTimeMinute = new ListBox();
    etTimeHPanel = new HorizontalPanel();
    stTimeHPanel = new HorizontalPanel();
    endTimeVPanel = new VerticalPanel();
    startTimeVPanel = new VerticalPanel();
    streamVPanel = new VerticalPanel();
    clusterVPanel = new VerticalPanel();
    stTimeLabel = new Label("Start Time");
    etTimeLabel = new Label("End Time");
    streamLabel = new Label("Stream");
    clusterLabel = new Label("Cluster");
    Button goButton = new Button("Go");

    startTime.getElement().setId("stTextBox");
    endtime.getElement().setId("etTextBox");
    streamsList.getElement().setId("streamDropDown");
    clusterList.getElement().setId("clusterDropDown");
    goButton.getElement().setId("goButton");
    filterPanel.getElement().setId("filterPanel");
    stDatePicker.setStyleName("calendar-popup");
    endDatePicker.setStyleName("calendar-popup");

    streamVPanel.add(streamLabel);
    streamVPanel.add(streamsList);
    clusterVPanel.add(clusterLabel);
    clusterVPanel.add(clusterList);
    stcalendarPopup.add(stDatePicker);
    etcalendarPopup.add(endDatePicker);
    stcalendarPopup.setGlassEnabled(true);
    etcalendarPopup.setGlassEnabled(true);

    startTime.addClickHandler(new ClickHandler() {
      public void onClick(ClickEvent event) {
        stcalendarPopup.center();
      }
    });
    endtime.addClickHandler(new ClickHandler() {
      public void onClick(ClickEvent event) {
        etcalendarPopup.center();
      }
    });
    stDatePicker.addValueChangeHandler(new ValueChangeHandler<Date>() {
      public void onValueChange(ValueChangeEvent<Date> event) {
        Date selectedDate = event.getValue();
        DateTimeFormat fmt = DateTimeFormat.getFormat(DateUtils.TEXTBOX_DATE_FORMAT);
        String selectedDateString = fmt.format(selectedDate);
        startTime.setText(selectedDateString);
        stcalendarPopup.hide();
      }
    });
    endDatePicker.addValueChangeHandler(new ValueChangeHandler<Date>() {
      public void onValueChange(ValueChangeEvent<Date> event) {
        Date selectedDate = event.getValue();
        DateTimeFormat fmt = DateTimeFormat.getFormat(DateUtils.TEXTBOX_DATE_FORMAT);
        String selectedDateString = fmt.format(selectedDate);
        endtime.setText(selectedDateString);
        etcalendarPopup.hide();
      }
    });
    startTime.setWidth("100px");
    endtime.setWidth("100px");
    stTimeHour.addItem("HH");
    stTimeHour.setWidth("50px");
    stTimeMinute.addItem("MM");
    stTimeMinute.setWidth("50px");
    edTimeHour.addItem("HH");
    edTimeHour.setWidth("50px");
    edTimeMinute.addItem("MM");
    edTimeMinute.setWidth("50px");
    fillListBox(stTimeHour, 24);
    fillListBox(stTimeMinute, 60);
    fillListBox(edTimeHour, 24);
    fillListBox(edTimeMinute, 60);

    streamsList.addItem("<Select Stream>");
    clusterList.addItem("<Select Cluster>");
    for (String stream : streams) {
      streamsList.addItem(stream);
    }
    for (String cluster : clusters) {
      clusterList.addItem(cluster);
    }
    goButton.addClickHandler(this);

    stTimeHPanel.add(startTime);
    stTimeHPanel.add(stTimeHour);
    stTimeHPanel.add(stTimeMinute);
    startTimeVPanel.add(stTimeLabel);
    startTimeVPanel.add(stTimeHPanel);

    etTimeHPanel.add(endtime);
    etTimeHPanel.add(edTimeHour);
    etTimeHPanel.add(edTimeMinute);
    endTimeVPanel.add(etTimeLabel);
    endTimeVPanel.add(etTimeHPanel);

    filterPanel.add(startTimeVPanel);
    filterPanel.add(endTimeVPanel);
    filterPanel.add(streamVPanel);
    filterPanel.add(clusterVPanel);
    filterPanel.add(goButton);
  }

  private void fillListBox(ListBox listBox, int value) {
    for (int i = 0; i < value; i++) {
      if (i < 10) {
        listBox.addItem("0" + String.valueOf(i));
      } else {
        listBox.addItem(String.valueOf(i));
      }
    }
  }

  public void onClick(ClickEvent event) {
    stTime = DateUtils.constructDateString(startTime.getText(),
        stTimeHour.getItemText(stTimeHour.getSelectedIndex()),
        stTimeMinute.getItemText(stTimeMinute.getSelectedIndex()));
    edTime = DateUtils.constructDateString(endtime.getText(),
        edTimeHour.getItemText(edTimeHour.getSelectedIndex()),
        edTimeMinute.getItemText(edTimeMinute.getSelectedIndex()));
    System.out.println("stTime: " + stTime);
    System.out.println("edTime: " + edTime);
    if (!validateParameters()) {
      return;
    }
    String selectedStream =
        streamsList.getItemText(streamsList.getSelectedIndex());
    String selectedCluster = clusterList.getItemText(clusterList.getSelectedIndex());
    String url = Window.Location.getHref();
    /*
      For running in GWT developement mode,
      url = url + "&qstart=" + stTime + "&qend=" + edTime +
        "&qcluster=" + selectedCluster +
        "&qstream=" + selectedStream;
     */
    url = url + "?qstart=" + stTime + "&qend=" + edTime +
        "&qcluster=" + selectedCluster +
        "&qstream=" + selectedStream;
    isReloaded = true;
    System.out.println("Replacing URL after adding selected parameters");
    Window.Location.replace(url);
  }

  public void sendRequest(String stTime, String edTime,
                          final String selectedCluster,
                          final String selectedStream, String nodesJson,
                          final String drillDownCluster,
                          final String drillDownStream) {
    System.out.println("Sending request to load graph");
    clearAndShowLoadingSymbol();
    String clientJson = ClientDataHelper.getInstance()
        .setGraphDataRequest(stTime, edTime, selectedStream, selectedCluster);
    serviceInstance.getData(clientJson, new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        caught.printStackTrace();
      }

      public void onSuccess(String result) {
        String nodesJson = ClientDataHelper.getInstance()
            .getJsonStrongFromGraphDataResponse(result);
        Map<String, Integer> slaMap = ClientDataHelper.getInstance()
            .getSlaMapFromGraphDataResponse(result);
        System.out.println("Json passed to js:" + nodesJson);
        drawGraph(nodesJson, selectedCluster, selectedStream,
            getQueryString(), drillDownCluster, drillDownStream,
            slaMap.get(ClientConstants.AGENT), slaMap.get(ClientConstants
            .VIP), slaMap.get(ClientConstants.COLLECTOR),
            slaMap.get(ClientConstants.HDFS), slaMap.get(ClientConstants
            .LOCAL), slaMap.get(ClientConstants.MERGE),
            slaMap.get(ClientConstants.MIRROR), ClientDataHelper.getInstance
            ().getPercentileForSlaFromGraphDataResponse(result), ClientDataHelper.getInstance
            ().getPercentageForLossFromGraphDataResponse(result));
      }
    });
  }

  private boolean validateParameters() {
    if (!DateUtils.checkTimeStringFormat(stTime)) {
      Window.alert("Incorrect format of startTime");
      return false;
    } else if (!DateUtils.checkTimeStringFormat(edTime)) {
      Window.alert("Incorrect format of endTime");
      return false;
    } else if (DateUtils.checkIfFutureDate(stTime)) {
      Window.alert("Future tart time is not allowed");
      return false;
    } else if (DateUtils.checkIfFutureDate(edTime)) {
      Window.alert("Future end time is not allowed");
      return false;
    } else if (DateUtils.checkStAfterEt(stTime, edTime)) {
      Window.alert("Start time is after end time");
      return false;
    } else if (streamsList.getSelectedIndex() < 1) {
      Window.alert("Select Stream");
      return false;
    } else if (clusterList.getSelectedIndex() < 1) {
      Window.alert("Select Cluster");
      return false;
    }
    return true;
  }

  private String getQueryString() {
    String gstream = Window.Location.getParameter(ClientConstants.GRAPH_STREAM);
    String gcluster = Window.Location.getParameter(ClientConstants.GRAPH_CLUSTER);
    if (gstream == null && gcluster == null) {
      return Window.Location.getQueryString();
    } else {
      return removeGraphParametersFromUrlQueryString();
    }
  }

  private String removeGraphParametersFromUrlQueryString() {
    String queryString = Window.Location.getQueryString();
    String[] parameters = queryString.split("&");
    String finalQueryString = "?";
    for (String parameter : parameters) {
      if (parameter.charAt(0) == '?') {
        parameter = parameter.substring(1);
      }
      String[] val = parameter.split("=");
      if (!val[0].equals(ClientConstants.GRAPH_CLUSTER) && !val[0].equals(ClientConstants.GRAPH_STREAM)) {
        if (finalQueryString.length() == 1) {
          finalQueryString += parameter;
        } else {
          finalQueryString += "&" + parameter;
        }
      }
    }
    return finalQueryString;
  }

  private native void clearAndShowLoadingSymbol()/*-{
    $wnd.clearSvgAndAddLoadSymbol();
  }-*/;

  private native void drawGraph(String result, String cluster, String stream,
                                String queryString, String drillDownCluster,
                                String drillDownStream, Integer agentSla,
                                Integer vipSla, Integer collectorSla,
                                Integer hdfsSla, Integer localSla,
                                Integer mergeSla, Integer mirrorSla,
                                Float percentileForSla,
                                Float percentageForSla)/*-{
    $wnd.drawGraph(result, cluster, stream, queryString, drillDownCluster,
    drillDownStream, agentSla, vipSla, collectorSla, hdfsSla, localSla,
    mergeSla, mirrorSla, percentileForSla, percentageForSla);
  }-*/;
}