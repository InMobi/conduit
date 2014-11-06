package com.inmobi.conduit.utils;

import java.util.Comparator;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.Partition;

import com.inmobi.conduit.AbstractService;

public class HCatPartitionComparator implements Comparator<Partition> {

  @Override
  public int compare(Partition part1, Partition part2) {
    Map<String, String> partSpec1 = part1.getSpec();
    Map<String, String> partSpec2 = part2.getSpec();
    int yearComp = partSpec1.get(AbstractService.YEAR_PARTITION_NAME).
        compareTo(partSpec2.get(AbstractService.YEAR_PARTITION_NAME));
    if (yearComp == 0) {
      int monthComp = partSpec1.get(AbstractService.MONTH_PARTITION_NAME).
          compareTo(partSpec2.get(AbstractService.MONTH_PARTITION_NAME));
      if (monthComp == 0) {
        int dayComp = partSpec1.get(AbstractService.DAY_PARTITION_NAME).
            compareTo(partSpec2.get(AbstractService.DAY_PARTITION_NAME));
        if (dayComp == 0) {
          int hourComp = partSpec1.get(AbstractService.HOUR_PARTITION_NAME).
              compareTo(partSpec2.get(AbstractService.HOUR_PARTITION_NAME));
          if (hourComp == 0) {
            return partSpec1.get(AbstractService.MINUTE_PARTITION_NAME).
                compareTo(partSpec2.get(AbstractService.MINUTE_PARTITION_NAME));
          } else {
            return hourComp;
          }
        } else {
          return dayComp;
        }
      } else {
        return monthComp;
      }
    } else {
      return yearComp;
    }
  }
}