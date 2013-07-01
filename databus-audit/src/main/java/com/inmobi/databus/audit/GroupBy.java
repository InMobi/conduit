package com.inmobi.databus.audit;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class GroupBy {

  @Override
  public String toString() {
    return "GroupBy" + isSet;
  }

  public class Group implements Comparable<Group> {
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getOuterType().hashCode();
      result = prime * result + ((columns == null) ? 0 : columns.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Group other = (Group) obj;
      if (!getOuterType().equals(other.getOuterType()))
        return false;
      if (columns == null) {
        if (other.columns != null)
          return false;
      } else if (!columns.equals(other.columns))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "" + columns;
    }

    private Map<Column, String> columns;

    public Group(Map<Column, String> values) {
      this.columns = values;
    }

    public String getCluster() {
      return columns.get(Column.CLUSTER);
    }

    public String getHostName() {
      return columns.get(Column.HOSTNAME);
    }

    public String getTier() {
      return columns.get(Column.TIER);
    }

    public String getTopic() {
      return columns.get(Column.TOPIC);
    }

    @Override
    public int compareTo(Group group) {
      int result = 0;
      if (columns.containsKey(Column.CLUSTER)) {
        String cluster1 = columns.get(Column.CLUSTER);
        String cluster2 = group.columns.get(Column.CLUSTER);
        result = cluster1.compareTo(cluster2);
        if (result != 0)
          return result;
      }
      if (columns.containsKey(Column.TIER)) {
        Tier tier1 = Tier.valueOf(columns.get(Column.TIER).toUpperCase());
        Tier tier2 = Tier.valueOf(group.columns.get(Column.TIER).toUpperCase());
        result = tier1.compareTo(tier2);
        if (result != 0)
          return result;
      }
      if (columns.containsKey(Column.TOPIC)) {
        String topic1 = columns.get(Column.TOPIC);
        String topic2 = group.columns.get(Column.TOPIC);
        result = topic1.compareTo(topic2);
        if (result != 0)
          return result;
      }
      if (columns.containsKey(Column.HOSTNAME)) {
        String hostname1 = columns.get(Column.HOSTNAME);
        String hostname2 = group.columns.get(Column.HOSTNAME);
        result = hostname1.compareTo(hostname2);
        if (result != 0)
          return result;
      }
      return 0;
    }

    private GroupBy getOuterType() {
      return GroupBy.this;
    }

  }

  private Set<Column> isSet;

  public GroupBy(String input) {
    isSet = new HashSet<Column>();
    if (input == null)
      return;
    String[] columns = input.split(",");
    for (String s : columns) {
      isSet.add(Column.valueOf(s.toUpperCase()));
    }
  }

  public Set<Column> getGroupByColumns() {
    return isSet;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((isSet == null) ? 0 : isSet.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    GroupBy other = (GroupBy) obj;
    if (isSet == null) {
      if (other.isSet != null)
        return false;
    } else if (!isSet.equals(other.isSet))
      return false;
    return true;
  }

  public Group getGroup(Map<Column, String> values) {
    values.keySet().retainAll(isSet);
    return new Group(values);
  }

}
