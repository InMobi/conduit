package com.inmobi.conduit.visualization.server;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.audit.Tier;
import com.inmobi.conduit.audit.Tuple;
import com.inmobi.conduit.audit.query.AuditDbQuery;
import com.inmobi.conduit.visualization.server.util.ServerDataHelper;
import com.inmobi.messaging.ClientConfig;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.util.*;

public class DataServiceManager {

  private static Logger LOG = Logger.getLogger(DataServiceManager.class);
  private static DataServiceManager instance = null;
  private List<ConduitConfig> conduitConfig;
  private VisualizationProperties properties;
  private final ClientConfig feederConfig;

  protected DataServiceManager(boolean init) {
    this(init, null, null);
  }

  protected DataServiceManager(boolean init,
                               String visualizationPropertiesPath,
                               String feederPropertiesPath) {
    if (feederPropertiesPath == null || feederPropertiesPath.length() == 0) {
      feederPropertiesPath = ServerConstants.FEEDER_PROPERTIES_DEFAULT_PATH;
    }
    feederConfig = ClientConfig.load(feederPropertiesPath);
    properties = new VisualizationProperties(visualizationPropertiesPath);
    if (init) {
      String folderPath = properties.get(ServerConstants.CONDUIT_XML_PATH);
      initConfig(folderPath);
    }
  }

  protected void initConfig(String folderPath) {
    File folder = new File(folderPath);
    File[] xmlFiles = folder.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        if (file.getName().toLowerCase().endsWith(".xml")) {
          return true;
        }
        return false;
      }
    });
    LOG.info("Conduit xmls included in the conf folder:");
    conduitConfig = new ArrayList<ConduitConfig>();
    for (File file : xmlFiles) {
      String fullPath = file.getAbsolutePath();
      LOG.info("File:" + fullPath);
      try {
        ConduitConfigParser parser = new ConduitConfigParser(fullPath);
        conduitConfig.add(parser.getConfig());
      } catch (Exception e) {
        LOG.error("Exception while intializing ConduitConfigParser: ", e);
      }
    }
  }

  public static DataServiceManager get(boolean init) {
    if (instance == null) {
      instance = new DataServiceManager(init);
    }
    return instance;
  }

  public String getStreamAndClusterList() {
    Set<String> streamSet = new TreeSet<String>();
    Set<String> clusterSet = new TreeSet<String>();
    for (ConduitConfig config : conduitConfig) {
      streamSet.addAll(config.getSourceStreams().keySet());
      clusterSet.addAll(config.getClusters().keySet());
    }
    streamSet.remove(ServerConstants.AUDIT_STREAM);
    List<String> streamList = new ArrayList<String>(streamSet);
    List<String> clusterList = new ArrayList<String>(clusterSet);
    streamList.add(0, "All");
    clusterList.add(0, "All");
    LOG.info("Returning stream list:" + streamList + " and cluster list:" +
        clusterList);
    String serverJson =
        ServerDataHelper.getInstance().setLoadMainPanelResponse(streamList,
            clusterList, properties, feederConfig);
    return serverJson;
  }

  public String getTopologyData(String filterValues) {
    Map<NodeKey, Node> nodeMap = new HashMap<NodeKey, Node>();
    Map<String, String> filterMap = getFilterMap(filterValues);
    String filterString = setFilterString(filterMap);
    AuditDbQuery dbQuery =
        new AuditDbQuery(filterMap.get(ServerConstants.END_TIME_FILTER),
            filterMap.get(ServerConstants.START_TIME_FILTER), filterString,
            ServerConstants.GROUPBY_STRING, ServerConstants.TIMEZONE,
            properties.get(ServerConstants.PERCENTILE_STRING), feederConfig);
    try {
      dbQuery.execute();
    } catch (Exception e) {
      LOG.error("Exception while executing query: ", e);
    }
    LOG.info("Audit topology query: " + dbQuery.toString());
    Set<Tuple> tupleSet = dbQuery.getTupleSet();
    Set<Float> percentileSet = dbQuery.getPercentileSet();
    Map<Tuple, Map<Float, Integer>> tuplesPercentileMap =
        dbQuery.getPercentile();
    LOG.debug("Percentile Set:" + percentileSet);
    LOG.debug("Tuples Percentile Map:" + tuplesPercentileMap);
    for (Tuple tuple : tupleSet) {
      createNode(tuple, nodeMap, percentileSet, tuplesPercentileMap.get(tuple));
    }
    buildPercentileMapOfAllNodes(nodeMap);
    addVIPNodesToNodesList(nodeMap, percentileSet);
    LOG.info("Final node list length:"+nodeMap.size());
    return ServerDataHelper.getInstance().setTopologyDataResponse(nodeMap);
  }

  protected Map<String, String> getFilterMap(String filterValues) {
    Map<String, String> filterMap = new HashMap<String, String>();
    filterMap.put(ServerConstants.STREAM_FILTER, ServerDataHelper.getInstance()
        .getStreamFromGraphDataReq(filterValues));
    filterMap.put(ServerConstants.CLUSTER_FILTER, ServerDataHelper.getInstance()
        .getColoFromGraphDataReq(filterValues));
    filterMap
        .put(ServerConstants.START_TIME_FILTER, ServerDataHelper.getInstance()
            .getStartTimeFromGraphDataReq(filterValues));
    filterMap
        .put(ServerConstants.END_TIME_FILTER, ServerDataHelper.getInstance()
            .getEndTimeFromGraphDataReq(filterValues));
    return filterMap;
  }

  protected void createNode(Tuple tuple, Map<NodeKey, Node> nodeMap,
                          Set<Float> percentileSet,
                          Map<Float, Integer> tuplePercentileMap) {
    LOG.debug("Creating node from tuple :" + tuple);
    String name, hostname = null;
    if (tuple.getTier().equalsIgnoreCase(Tier.HDFS.toString())) {
      name = tuple.getCluster();
      hostname = tuple.getHostname();
    } else {
      name = tuple.getHostname();
    }
    MessageStats receivedMessageStat =
        new MessageStats(tuple.getTopic(), tuple.getReceived(), hostname);
    MessageStats sentMessageStat =
        new MessageStats(tuple.getTopic(), tuple.getSent(), hostname);
    NodeKey newNodeKey = new NodeKey(name, tuple.getCluster(), tuple.getTier());
    List<MessageStats> receivedMessageStatsList = new ArrayList<MessageStats>(),
        sentMessageStatsList = new ArrayList<MessageStats>();
    receivedMessageStatsList.add(receivedMessageStat);
    sentMessageStatsList.add(sentMessageStat);
    Node node = nodeMap.get(newNodeKey);
    if (node == null) {
      node = new Node(name, tuple.getCluster(), tuple.getTier());
    }
    if (node.getReceivedMessagesList().size() > 0) {
      receivedMessageStatsList.addAll(node.getReceivedMessagesList());
    }
    if (node.getSentMessagesList().size() > 0) {
      sentMessageStatsList.addAll(node.getSentMessagesList());
    }
    if (tuple.getTier().equalsIgnoreCase(Tier.MERGE.toString()) || tuple
        .getTier().equalsIgnoreCase(Tier.MIRROR.toString())) {
      Set<String> sourceList = getSourceListForTuple(tuple);
      node.setSourceListForTopic(tuple.getTopic(), sourceList);
    }
    node.setReceivedMessagesList(receivedMessageStatsList);
    node.setSentMessagesList(sentMessageStatsList);
    node.setPercentileSet(percentileSet);
    node.addToTopicPercentileMap(tuple.getTopic(), tuplePercentileMap);
    node.addToTopicCountMap(tuple.getTopic(), tuple.getLatencyCountMap());
    nodeMap.put(newNodeKey, node);
    LOG.debug("Node created: " + node);
  }

  private Set<String> getSourceListForTuple(Tuple tuple) {
    LOG.info("Setting source list for tuple:" + tuple);
    Set<String> sourceList = new HashSet<String>();
    String clusterName = tuple.getCluster();
    for (ConduitConfig config : conduitConfig) {
      Map<String, Cluster> clusterMap = config.getClusters();
      Cluster cluster = clusterMap.get(clusterName);
      if (cluster == null) {
        LOG.debug("Could not find cluster of tuple:"+tuple+" in clusterMap");
        continue;
      }
      if (tuple.getTier().equalsIgnoreCase(Tier.MERGE.toString())) {
        Set<String> mergeStreams = cluster.getPrimaryDestinationStreams();
        for (Map.Entry<String, Cluster> entry: clusterMap.entrySet()) {
          for (String stream: mergeStreams) {
            if (entry.getValue().getSourceStreams().contains(stream)) {
              sourceList.add(entry.getKey());
            }
          }
        }
      } else if (tuple.getTier().equalsIgnoreCase(Tier.MIRROR.toString())) {
        Set<String> mirroredStreams = cluster.getMirroredStreams();
        for (Map.Entry<String, Cluster> entry: clusterMap.entrySet()) {
          for (String stream: mirroredStreams) {
            if (entry.getValue().getPrimaryDestinationStreams().contains(stream)) {
              sourceList.add(entry.getKey());
            }
          }
        }

      }
    }
    LOG.info("Set source list for tuple " + tuple + "as:" + sourceList);
    return sourceList;
  }

  protected String setFilterString(Map<String, String> filterMap) {
    String filterString;
    String selectedStream = filterMap.get(ServerConstants.STREAM_FILTER);
    String selectedCluster = filterMap.get(ServerConstants.CLUSTER_FILTER);
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

  private void buildPercentileMapOfAllNodes(Map<NodeKey, Node> nodeMap) {
    for (Node node : nodeMap.values()) {
      node.buildPercentileMap();
    }
  }

  protected void addVIPNodesToNodesList(Map<NodeKey, Node> nodeMap,
                                      Set<Float> percentileSet) {
    Map<String, Node> vipNodeMap = new HashMap<String, Node>();
    for (Node node : nodeMap.values()) {
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
      entry.getValue().buildPercentileMap();
      nodeMap.put(entry.getValue().getNodeKey(), entry.getValue());
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
    LOG.debug("List1: " + list1 + " List2: " + list2 + " MERGRED LIST : " +
        mergedList);
    return mergedList;
  }

  public List<ConduitConfig> getConduitConfig() {
    return Collections.unmodifiableList(conduitConfig);
  }
  public String getTierLatencyData(String filterValues) {
    Map<String, String> filterMap = getFilterMap(filterValues);
    String filterString = setFilterString(filterMap);
    Map<Tuple, Map<Float, Integer>> tierLatencyMap = getTierLatencyMap
        (filterMap.get(ServerConstants.END_TIME_FILTER),
            filterMap.get(ServerConstants.START_TIME_FILTER), filterString);
    return ServerDataHelper.getInstance().setTierLatencyResponseObject(tierLatencyMap, properties);
  }

  private Map<Tuple, Map<Float, Integer>> getTierLatencyMap(String endTime,
                                                            String startTime,
                                                            String
                                                                filterString) {
    AuditDbQuery dbQuery = new AuditDbQuery(endTime, startTime, filterString,
        "TIER", ServerConstants.TIMEZONE, properties.get(ServerConstants
        .PERCENTILE_FOR_SLA), feederConfig);
    try {
      dbQuery.execute();
    } catch (Exception e) {
      LOG.error("Exception while executing query: ", e);
    }
    LOG.info("Audit latency summary query: " + dbQuery.toString());
    return dbQuery.getPercentile();
  }

  /**
   * Will retrieve the timeseries information for a date range
   */
  public String getTimeLineData(String filterValues) {
    Map<String, String> filterMap = getFilterMap(filterValues);
    String filterString = setFilterString(filterMap);
    AuditDbQuery dbQuery =
        new AuditDbQuery(filterMap.get(ServerConstants.END_TIME_FILTER),
            filterMap.get(ServerConstants.START_TIME_FILTER), filterString,
            ServerConstants.GROUPBY_TIMELINE_STRING, ServerConstants.TIMEZONE,
            properties.get(ServerConstants.PERCENTILE_STRING), feederConfig);
    try {
      LOG.debug("Executing time line query");
      dbQuery.execute();
      LOG.info("Audit Time line query: " + dbQuery.toString());
      LOG.debug("Executed time line query");
    } catch (Exception e) {
      LOG.error("Exception while executing time line query: ", e);
      return null;
    }
    return ServerDataHelper.setTimeLineDataResponse(ServerDataHelper
        .convertToJson(dbQuery));
  }
}