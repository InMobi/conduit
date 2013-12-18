package com.inmobi.databus.visualization.server;

public class NodeKey {
  private final String hostname, tier, cluster;

  public NodeKey(String hostname, String cluster, String tier) {
    this.tier = tier;
    this.cluster = cluster;
    this.hostname = hostname;
  }

  public String getHostname() {
    return hostname;
  }

  public String getTier() {
    return tier;
  }

  public String getCluster() {
    return cluster;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NodeKey nodeKey = (NodeKey) o;

    if (!cluster.equals(nodeKey.cluster)) {
      return false;
    }
    if (!hostname.equals(nodeKey.hostname)) {
      return false;
    }
    if (!tier.equals(nodeKey.tier)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = hostname.hashCode();
    result = 31 * result + tier.hashCode();
    result = 31 * result + cluster.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "NodeKey{" +
        "hostname='" + hostname + '\'' +
        ", tier='" + tier + '\'' +
        ", cluster='" + cluster + '\'' +
        '}';
  }
}
