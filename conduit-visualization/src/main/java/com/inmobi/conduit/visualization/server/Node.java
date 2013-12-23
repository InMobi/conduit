package com.inmobi.conduit.visualization.server;

import com.inmobi.conduit.audit.LatencyColumns;

import java.util.*;

public class Node {
  private final NodeKey nodeKey;
  private Long aggregateMessagesReceived = 0L;
  private Long aggregateMessagesSent = 0L;
  private List<MessageStats> receivedMessagesList;
  private List<MessageStats> sentMessagesList;
  private List<String> sourceList;
  private Set<Float> percentileSet;
  private Map<String, Map<LatencyColumns, Long>> perTopicCountMap = new
      HashMap<String, Map<LatencyColumns, Long>>();
  private Map<String, Map<Float, Integer>> perTopicPercentileMap = new
      HashMap<String, Map<Float, Integer>>();
  private Map<LatencyColumns, Long> latencyValues = new HashMap
      <LatencyColumns, Long>();
  private Map<Float, Integer> percentileMap = new HashMap<Float, Integer>();

  public Node(String name, String clusterName, String tier) {
    this.nodeKey = new NodeKey(name, clusterName, tier);
    receivedMessagesList = new ArrayList<MessageStats>();
    sentMessagesList = new ArrayList<MessageStats>();
  }

  private Long calculateAggregateMessages(List<MessageStats> messageStatsList) {
    Long aggregateMessages = 0L;
    for (MessageStats stats : messageStatsList) {
      aggregateMessages += stats.getMessages();
    }
    return aggregateMessages;
  }

  public void addToTopicPercentileMap(String topic, Map<Float,
      Integer> percentileMap) {
    perTopicPercentileMap.put(topic, percentileMap);
  }

  public void addLatencyValue(LatencyColumns column, Long value) {
    Long prevVal = latencyValues.get(column);
    if (prevVal == null)
      prevVal = 0l;
    if (value == null)
      value = 0l;
    latencyValues.put(column, prevVal + value);
  }

  public Map<Float, Integer> populatePercentileMap(Map<LatencyColumns,
      Long> latencyCountMap) {
    Map<Float, Integer> map = new HashMap<Float, Integer>();
    Long totalCount = 0l;
    for (LatencyColumns columns : LatencyColumns.values()) {
      Long val = latencyCountMap.get(columns);
      if (val == null)
        val = 0l;
      totalCount += val;
    }
    Long finalCount = totalCount - latencyCountMap.get(LatencyColumns.C600);
    Long currentCount = 0l;
    Iterator<Float> it = percentileSet.iterator();
    Float currentPercentile = it.next();
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      if (latencyColumn == LatencyColumns.C600)
        continue;
      Long value = latencyCountMap.get(latencyColumn);
      while (currentCount + value >= ((currentPercentile * finalCount) / 100)) {
        map.put(currentPercentile, latencyColumn.getValue());
        if(it.hasNext())
          currentPercentile = it.next();
        else
          break;
      }
      if (!it.hasNext() && map.get(currentPercentile) != null)
        break;
      currentCount += value;
    }
    return map;
  }

  public void addToTopicCountMap(String topic, Map<LatencyColumns,
      Long> latencyCountMap) {
    Map<LatencyColumns, Long> finalMap = new HashMap<LatencyColumns, Long>();
    Map<LatencyColumns, Long> prevMap = perTopicCountMap.get(topic);
    if (prevMap == null) {
      finalMap.putAll(latencyCountMap);
      perTopicCountMap.put(topic, finalMap);
    } else {
      for (LatencyColumns columns : LatencyColumns.values()) {
        Long prevVal = prevMap.get(columns);
        Long currVal = latencyCountMap.get(columns);
        if (prevVal == null)
          prevVal = 0l;
        if (currVal == null)
          currVal = 0l;
        finalMap.put(columns, prevVal + currVal);
      }
      perTopicCountMap.put(topic, finalMap);
    }
  }

  public void buildPercentileMap() {
    /*
    Build perTopicPercentileMap from perTopicCountMap,
    aggreagate stats and populate latencyValues and finally create
    percentileMap for the node.
    */
    buildPerTopicPercentileMapFromCountMap();
    populateLatencyValues();
    percentileMap = populatePercentileMap(latencyValues);

  }

  private void populateLatencyValues() {
    for (Map.Entry<String, Map<LatencyColumns, Long>> topicMapEntry :
        perTopicCountMap.entrySet()) {
      Map<LatencyColumns, Long> latencyCountMap = topicMapEntry.getValue();
      for (LatencyColumns columns : LatencyColumns.values()) {
        addLatencyValue(columns, latencyCountMap.get(columns));
      }
    }
  }

  private void buildPerTopicPercentileMapFromCountMap() {
    for (Map.Entry<String, Map<LatencyColumns, Long>> topicMapEntry :
        perTopicCountMap.entrySet()) {
      Map<Float, Integer> topicPercentileMap = populatePercentileMap
          (topicMapEntry.getValue());
      perTopicPercentileMap.put(topicMapEntry.getKey(), topicPercentileMap);
    }
  }

  public Map<String, Map<LatencyColumns, Long>> getPerTopicCountMap() {
    return perTopicCountMap;
  }

  public void addAllTopicCountMaps(
      Map<String, Map<LatencyColumns, Long>> allTopicsCountMap) {
    for (Map.Entry<String, Map<LatencyColumns, Long>> topicMapEntry :
        allTopicsCountMap.entrySet()) {
      String topic = topicMapEntry.getKey();
      Map<LatencyColumns, Long> finalMap = new HashMap<LatencyColumns, Long>();
      Map<LatencyColumns, Long> currentMap = topicMapEntry.getValue();
      Map<LatencyColumns, Long> prevMap = perTopicCountMap.get(topicMapEntry
          .getKey());
      if (prevMap == null) {
        finalMap.putAll(currentMap);
        perTopicCountMap.put(topic, finalMap);
        continue;
      }
      for (LatencyColumns columns : LatencyColumns.values()) {
        Long prevVal = prevMap.get(columns);
        Long currentVal = currentMap.get(columns);
        if (prevVal == null)
          prevVal = 0l;
        if (currentVal == null)
          currentVal = 0l;
        finalMap.put(columns, prevVal + currentVal);
      }
      perTopicCountMap.put(topic, finalMap);
    }
  }

  public String getName() {
    return nodeKey.getHostname();
  }

  public String getClusterName() {
    return nodeKey.getCluster();
  }

  public Long getAggregateMessagesReceived() {
    return aggregateMessagesReceived;
  }

  public String getTier() {
    return nodeKey.getTier();
  }

  public Long getAggregateMessagesSent() {
    return aggregateMessagesSent;
  }

  public List<MessageStats> getReceivedMessagesList() {
    return receivedMessagesList;
  }

  public List<MessageStats> getSentMessagesList() {
    return sentMessagesList;
  }

  public List<String> getSourceList() {
    return sourceList;
  }

  public Map<String, Map<Float, Integer>> getPerTopicPercentileMap() {
    return perTopicPercentileMap;
  }

  public Map<Float, Integer> getPercentileMap() {
    return percentileMap;
  }

  public NodeKey getNodeKey() {
    return nodeKey;
  }

  public void setAggregateMessagesSent(Long aggregateMessagesSent) {
    this.aggregateMessagesSent = aggregateMessagesSent;
  }

  public void setAggregateMessagesReceived(Long aggregateMessagesReceived) {
    this.aggregateMessagesReceived = aggregateMessagesReceived;
  }

  public void setReceivedMessagesList(List<MessageStats> receivedMessagesList) {
    this.receivedMessagesList = receivedMessagesList;
    if (this.receivedMessagesList.size() > 0) {
      aggregateMessagesReceived =
          calculateAggregateMessages(this.receivedMessagesList);
    }
  }

  public void setSentMessagesList(List<MessageStats> sentMessagesList) {
    this.sentMessagesList = sentMessagesList;
    if (this.sentMessagesList.size() > 0) {
      aggregateMessagesSent = calculateAggregateMessages(this.sentMessagesList);
    }
  }

  public void setPercentileMap(Map<Float, Integer> percentileMap) {
    this.percentileMap = percentileMap;
  }

  public void setSourceList(Set<String> sourceList) {
    this.sourceList = new ArrayList<String>();
    this.sourceList.addAll(sourceList);
  }

  public void setPercentileSet(Set<Float> percentileSet) {
    this.percentileSet = percentileSet;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Node node = (Node) o;

    if (!nodeKey.equals(node.nodeKey)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return nodeKey.hashCode();
  }

  @Override
  public String toString() {
    return "Node{" +
        "nodeKey=" + nodeKey +
        ", aggregateMessagesReceived=" + aggregateMessagesReceived +
        ", aggregateMessagesSent=" + aggregateMessagesSent +
        ", receivedMessagesList=" + receivedMessagesList +
        ", sentMessagesList=" + sentMessagesList +
        ", sourceList=" + sourceList +
        ", percentileSet=" + percentileSet +
        ", perTopicCountMap=" + perTopicCountMap +
        ", perTopicPercentileMap=" + perTopicPercentileMap +
        ", latencyValues=" + latencyValues +
        ", percentileMap=" + percentileMap +
        '}';
  }
}
