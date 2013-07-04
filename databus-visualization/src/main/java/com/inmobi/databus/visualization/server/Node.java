package com.inmobi.databus.visualization.server;

import com.inmobi.databus.audit.LatencyColumns;

import java.util.*;

public class Node {
  private String name;
  private String clusterName;
  private String tier;
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
    this.name = name;
    this.clusterName = clusterName;
    this.tier = tier;
    receivedMessagesList = new ArrayList<MessageStats>();
    sentMessagesList = new ArrayList<MessageStats>();
  }

  public List<MessageStats> getReceivedMessagesList() {
    return receivedMessagesList;
  }

  public void setReceivedMessagesList(List<MessageStats> receivedMessagesList) {
    this.receivedMessagesList = receivedMessagesList;
    if (this.receivedMessagesList.size() > 0) {
      aggregateMessagesReceived =
          calculateAggregateMessages(this.receivedMessagesList);
    }
  }

  public List<MessageStats> getSentMessagesList() {
    return sentMessagesList;
  }

  public void setSentMessagesList(List<MessageStats> sentMessagesList) {
    this.sentMessagesList = sentMessagesList;
    if (this.sentMessagesList.size() > 0) {
      aggregateMessagesSent = calculateAggregateMessages(this.sentMessagesList);
    }
  }

  private Long calculateAggregateMessages(List<MessageStats> messageStatsList) {
    Long aggregateMessages = 0L;
    for (MessageStats stats : messageStatsList) {
      aggregateMessages += stats.getMessages();
    }
    return aggregateMessages;
  }

  public String getName() {
    return name;
  }

  public String getClusterName() {
    return clusterName;
  }

  public Long getAggregateMessagesReceived() {
    return aggregateMessagesReceived;
  }

  public String getTier() {
    return tier;
  }

  public Long getAggregateMessagesSent() {
    return aggregateMessagesSent;
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

    if (!clusterName.equals(node.clusterName)) {
      return false;
    }
    if (!name.equals(node.name)) {
      return false;
    }
    if (!tier.equals(node.tier)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + clusterName.hashCode();
    result = 31 * result + tier.hashCode();
    return result;
  }

  public List<String> getSourceList() {
    return sourceList;
  }

  public void setSourceList(Set<String> sourceList) {
    this.sourceList = new ArrayList<String>();
    this.sourceList.addAll(sourceList);
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

  public void setPercentileSet(Set<Float> percentileSet) {
    this.percentileSet = percentileSet;
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

  public void buildPercentileMap(boolean isBuildPerTopic) {
    /*
    Build perTopicPercentileMap from perTopicCountMap if isBuildPerTopic is
    true, aggreagate stats and populate latencyValues and finally create
    percentileMap for the node.
    */
    if (isBuildPerTopic)
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

  public Map<String, Map<Float, Integer>> getPerTopicPercentileMap() {
    return perTopicPercentileMap;
  }

  public Map<Float, Integer> getPercentileMap() {
    return percentileMap;
  }

  @Override
  public String toString() {
    return "Node{" +
        "name='" + name + '\'' +
        ", clusterName='" + clusterName + '\'' +
        ", tier='" + tier + '\'' +
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
