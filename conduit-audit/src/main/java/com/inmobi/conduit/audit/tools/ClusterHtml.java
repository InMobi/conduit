package com.inmobi.conduit.audit.tools;

public class ClusterHtml {

  private String cluster;
  private StringBuilder localHtmlBody;
  private StringBuilder mergeHtmlBody;
  protected static boolean isStartedProcessing = false;

  public ClusterHtml(String cluster) {
    this.cluster = cluster;
    localHtmlBody = new StringBuilder();
    mergeHtmlBody = new StringBuilder();
  }

  protected String getCluster() {
    return cluster;
  }

  protected StringBuilder getLocalHtmlBody() {
    return localHtmlBody;
  }

  protected void setLocalHtmlBody(StringBuilder localHtmlBody) {
    this.localHtmlBody = localHtmlBody;
  }

  protected StringBuilder getMergeHtmlBody() {
    return mergeHtmlBody;
  }

  protected void setMergeHtmlBody(StringBuilder mergeHtmlBody) {
    this.mergeHtmlBody = mergeHtmlBody;
  }

  protected void updateLocalMergeHtmlWithRowSpan(String cluster,
      int localRowCount, int mergeRowCount) {
    int total = localRowCount + mergeRowCount;
    localHtmlBody.insert(0," <th rowspan=" + localRowCount + ">" + "LOCAL </th>");
    localHtmlBody.insert(0,"<tr> <th rowspan=" + total + ">" +  cluster +" </th>");
    localHtmlBody.append("</tr>").append("\n");
    mergeHtmlBody.insert(0,"<tr> <th rowspan=" + mergeRowCount + ">" + "MERGE </th>");
    mergeHtmlBody.append("</tr>").append("\n");
  }

  protected void updateMergeHtmlBodyWithRows(String value) {
    mergeHtmlBody.append("</tr> \n <tr>").append("\n");
    prepareMergeTableRowData(value);
  }

  protected void updateLocalHtmlBodyWithRows(String value) {
    localHtmlBody.append("</tr> \n <tr>").append("\n");
    prepareLocalTableRowData(value);
  }

  protected void prepareMergeTableRowData(String value) {
    mergeHtmlBody.append("<td>" + value + "</td>").append("\n");
  }

  protected void prepareLocalTableRowData(String value) {
    localHtmlBody.append("<td>" + value + "</td>").append("\n");
  }
}
