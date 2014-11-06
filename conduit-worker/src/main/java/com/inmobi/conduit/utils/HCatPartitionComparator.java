package com.inmobi.conduit.utils;

import java.util.Comparator;

import org.apache.hive.hcatalog.api.HCatPartition;

public class HCatPartitionComparator implements Comparator<HCatPartition> {

  @Override
  public int compare(HCatPartition part1, HCatPartition part2) {
    String location1 = part1.getLocation();
    String location2 = part2.getLocation();
    if (location1 != null && location2 != null) {
      return location1.compareTo(location2);
    }
    return -1;
  }
}