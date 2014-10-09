package com.inmobi.conduit.utils;

import java.util.Comparator;

import org.apache.hadoop.hive.ql.metadata.Partition;

public class HCatPartitionComparator implements Comparator<Partition> {

  @Override
  public int compare(Partition part1, Partition part2) {
    String location1 = part1.getLocation();
    String location2 = part2.getLocation();
    if (location1 != null && location2 != null) {
      return location1.compareTo(location2);
    }
    return -1;
  }
}