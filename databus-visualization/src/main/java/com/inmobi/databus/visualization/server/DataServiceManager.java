package com.inmobi.databus.visualization.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.audit.Tier;
import com.inmobi.databus.audit.Tuple;
import com.inmobi.databus.audit.query.AuditDbQuery;
import com.inmobi.databus.visualization.server.util.ServerDataHelper;

public class DataServiceManager {
  public static final String GROUPBY_STRING = "TIER,HOSTNAME,TOPIC,CLUSTER";
  public static final String TIMEZONE = "GMT";
  private static Logger LOG = Logger.getLogger(DataServiceManager.class);
  private static DataServiceManager instance = null;
  private DatabusConfig dataBusConfig;

  private DataServiceManager() {
    String filename = VisualizationProperties
        .get(VisualizationProperties.PropNames.DATABUS_XML_PATH);
    try {
      DatabusConfigParser parser = new DatabusConfigParser(filename);
      dataBusConfig = parser.getConfig();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static DataServiceManager get() {
    if (instance == null) {
      instance = new DataServiceManager();
    }
    return instance;
  }

  public String getStreamAndClusterList() {
    List<String> streamList = new ArrayList<String>();
    streamList.addAll(dataBusConfig.getSourceStreams().keySet());
    streamList.add(0, "All");
    List<String> clusterList = new ArrayList<String>();
    clusterList.addAll(dataBusConfig.getClusters().keySet());
    clusterList.add(0, "All");
    String serverJson =
        ServerDataHelper.getInstance().setLoadMainPanelResponse(streamList,
            clusterList);
    return serverJson;
  }

  public String getData(String filterValues) {
    List<Node> nodeList = new ArrayList<Node>();
    String responseJson;
    String selectedStream =
        ServerDataHelper.getInstance().getStreamFromGraphDataReq(filterValues);
    String selectedCluster =
        ServerDataHelper.getInstance().getColoFromGraphDataReq(filterValues);
    String startTime = ServerDataHelper.getInstance()
        .getStartTimeFromGraphDataReq(filterValues);
    String endTime =
        ServerDataHelper.getInstance().getEndTimeFromGraphDataReq(filterValues);
    String filterString = setFilterString(selectedStream, selectedCluster);
    AuditDbQuery dbQuery = new AuditDbQuery(endTime, startTime, filterString,
        GROUPBY_STRING, TIMEZONE, VisualizationProperties.get(
        VisualizationProperties.PropNames.PERCENTILE_STRING));
    try {
      dbQuery.execute();
    } catch (Exception e) {
      e.printStackTrace();
    }
    LOG.info("Audit query: " + dbQuery.toString());
    dbQuery.displayResults();
    Set<Tuple> tupleSet = dbQuery.getTupleSet();
    Set<Float> percentileSet = dbQuery.getPercentileSet();
    Map<Tuple, Map<Float, Integer>> tuplesPercentileMap = dbQuery.getPercentile();
    LOG.debug("Percentile Set:"+percentileSet);
    LOG.debug("Tuples Percentile Map:"+tuplesPercentileMap);
    for (Tuple tuple : tupleSet) {
      LOG.info("Creating node from tuple :"+tuple);
      String name, hostname = null;
      if(tuple.getTier().equalsIgnoreCase(Tier.HDFS.toString())) {
        name = tuple.getCluster();
        hostname = tuple.getHostname();
      } else
        name = tuple.getHostname();
      MessageStats receivedMessageStat =
          new MessageStats(tuple.getTopic(), tuple.getReceived(), hostname);
      MessageStats sentMessageStat =
          new MessageStats(tuple.getTopic(), tuple.getSent(), hostname);
      Node newNode = new Node(name, tuple.getCluster(), tuple.getTier());
      Node finalNode = null;
      List<MessageStats> receivedMessageStatsList = new ArrayList<MessageStats>(),
          sentMessageStatsList = new ArrayList<MessageStats>();
      receivedMessageStatsList.add(receivedMessageStat);
      sentMessageStatsList.add(sentMessageStat);
      if (nodeList.contains(newNode)) {
        for (Node currentNode : nodeList) {
          if (newNode.equals(currentNode)) {
            if (currentNode.getReceivedMessagesList().size() > 0) {
              receivedMessageStatsList
                  .addAll(currentNode.getReceivedMessagesList());
            }
            if (currentNode.getSentMessagesList().size() > 0) {
              sentMessageStatsList.addAll(currentNode.getSentMessagesList());
            }
            finalNode = currentNode;
            break;
          }
        }
      }
      if (nodeList.isEmpty() || finalNode == null) {
        nodeList.add(newNode);
        finalNode = newNode;
      }
      finalNode.setReceivedMessagesList(receivedMessageStatsList);
      finalNode.setSentMessagesList(sentMessageStatsList);
      finalNode.setPercentileSet(percentileSet);
      finalNode.addToTopicPercentileMap(tuple.getTopic(),
          tuplesPercentileMap.get(tuple));
      finalNode.addToTopicCountMap(tuple.getTopic(), tuple.getLatencyCountMap());
      LOG.info("Node created: " + finalNode);
    }
    buildPercentileMapOfAllNodes(nodeList);
    addVIPNodesToNodesList(nodeList, percentileSet);
    checkAndSetSourceListForMergeMirror(nodeList);
    LOG.debug("Printing node list");
    for (Node node : nodeList) {
      LOG.debug("Final node :" + node);
    }
    responseJson = ServerDataHelper.getInstance().setGraphDataResponse(nodeList);
    LOG.debug("Json response returned to client : " + responseJson);
    return responseJson;
  }

  private String setFilterString(String selectedStream,
                                 String selectedCluster) {
    String filterString;
    if (selectedStream.compareTo("All") == 0) {
      filterString = null;
    } else {
      filterString = "TOPIC=" + selectedStream;
    }
    if (!selectedCluster.equalsIgnoreCase("All")) {
      if (filterString == null || filterString.isEmpty()) {
        filterString = "CLUSTER=" + selectedCluster;
      } else {
        filterString += ",CLUSTER=" + selectedCluster;
      }
    }
    return filterString;
  }

  private void buildPercentileMapOfAllNodes(List<Node> nodeList) {
    for (Node node: nodeList)
      node.buildPercentileMap(false);
  }

  private void addVIPNodesToNodesList(List<Node> nodeList,
                                      Set<Float> percentileSet) {
    Map<String, Node> vipNodeMap = new HashMap<String, Node>();
    for (Node node : nodeList) {
      if (node.getTier().equalsIgnoreCase(Tier.COLLECTOR.toString())) {
        Node vipNode = vipNodeMap.get(node.getClusterName());
        if (vipNode == null) {
          vipNode =
              new Node(node.getClusterName(), node.getClusterName(), "VIP");
          vipNode.setPercentileSet(percentileSet);
        }
        vipNode.setReceivedMessagesList(
            mergeLists(vipNode.getReceivedMessagesList(),
                node.getReceivedMessagesList()));
        vipNode.setSentMessagesList(mergeLists(vipNode.getSentMessagesList(),
            node.getSentMessagesList()));
        vipNode.addAllTopicCountMaps(node.getPerTopicCountMap());
        vipNodeMap.put(node.getClusterName(), vipNode);
      }
    }
    for (Map.Entry<String, Node> entry : vipNodeMap.entrySet()) {
      entry.getValue().buildPercentileMap(true);
      nodeList.add(entry.getValue());
    }
  }

  private List<MessageStats> mergeLists(List<MessageStats> list1,
                                        List<MessageStats> list2) {
    List<MessageStats> mergedList = new ArrayList<MessageStats>();
    if (list1.isEmpty() && !list2.isEmpty()) {
      for (MessageStats stats : list2) {
        mergedList.add(new MessageStats(stats));
      }
    } else if (!list1.isEmpty() && list2.isEmpty()) {
      for (MessageStats stats : list1) {
        mergedList.add(new MessageStats(stats));
      }
    } else {
      mergedList.addAll(list1);
      for (MessageStats stats : list2) {
        Long finalCount = stats.getMessages();
        boolean isPresent = false;
        for (MessageStats comparestats : mergedList) {
          if (comparestats.getTopic().equalsIgnoreCase(stats.getTopic())) {
            finalCount += comparestats.getMessages();
            comparestats.setMessages(finalCount);
            isPresent = true;
            break;
          }
        }
        if (!isPresent) {
          mergedList.add(new MessageStats(stats));
        }
      }
    }
    LOG.debug("List1: " + list1);
    LOG.debug("List2: " + list2);
    LOG.debug("MERGRED LIST : " + mergedList);
    return mergedList;
  }

  /**
   * If any node in the nodeList is a merge/mirror tier node, set the source
   * node's names' list for it.
   *
   * @param nodeList
   */
  private void checkAndSetSourceListForMergeMirror(List<Node> nodeList) {
    for (Node node : nodeList) {
      if (node.getTier().equalsIgnoreCase("merge") ||
          node.getTier().equalsIgnoreCase("mirror")) {
        node.setSourceList(
            dataBusConfig.getClusters().get(node.getClusterName())
                .getDestinationStreams().keySet());
      }
    }
  }
}