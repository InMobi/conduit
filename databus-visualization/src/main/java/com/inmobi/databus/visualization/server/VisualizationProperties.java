package com.inmobi.databus.visualization.server;

import sun.management.resources.agent;

import java.io.File;
import java.io.FileInputStream;
import java.util.Hashtable;
import java.util.Properties;

public class VisualizationProperties {
  public static enum PropNames {
    DATABUS_XML_PATH,
    PERCENTILE_STRING,
    AGENT_SLA,
    VIP_SLA,
    COLLECTOR_SLA,
    HDFS_SLA,
    LOCAL_SLA,
    MERGE_SLA,
    MIRROR_SLA,
    PERCENTILE_FOR_SLA,
    PERCENTAGE_FOR_LOSS,
    PERCENTAGE_FOR_WARN,
    MAX_START_TIME,
    MAX_TIME_RANGE_INTERVAL_IN_HOURS
  }

  private static Hashtable<String, String> propMap =
      new Hashtable<String, String>();

  static {
    try {
      Properties p = new Properties();
      p.load(new FileInputStream(
          new File("/usr/local/databus-visualization/conf/visualization.properties")));
      propMap.put(PropNames.DATABUS_XML_PATH.name(),
          p.get("databus.xml.path").toString());
      propMap.put(PropNames.PERCENTILE_STRING.name(),
          p.get("percentile.string").toString());
      propMap.put(PropNames.AGENT_SLA.name(),
          p.get("agent.sla").toString());
      propMap.put(PropNames.VIP_SLA.name(), p.get("vip.sla").toString());
      propMap.put(PropNames.COLLECTOR_SLA.name(),
          p.get("collector.sla").toString());
      propMap.put(PropNames.HDFS_SLA.name(),
          p.get("hdfs.sla").toString());
      propMap.put(PropNames.LOCAL_SLA.name(),
          p.get("local.sla").toString());
      propMap.put(PropNames.MERGE_SLA.name(),
          p.get("merge.sla").toString());
      propMap.put(PropNames.MIRROR_SLA.name(),
          p.get("mirror.sla").toString());
      propMap.put(PropNames.PERCENTILE_FOR_SLA.name(),
          p.get("percentile.for.sla").toString());
      propMap.put(PropNames.PERCENTAGE_FOR_LOSS.name(),
          p.get("percentage.for.loss").toString());
      propMap.put(PropNames.PERCENTAGE_FOR_WARN.name(),
          p.get("percentage.for.warn").toString());
      propMap.put(PropNames.MAX_START_TIME.name(),
          p.get("max.start.time").toString());
      propMap.put(PropNames.MAX_TIME_RANGE_INTERVAL_IN_HOURS.name(),
          p.get("max.time.range.interval.in.hours").toString());
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(
          "Error while initializing VisualizationProperties:" +
              " " + e.getMessage());
    }
  }

  public static boolean set(String key, String value) {
    try {
      propMap.put(PropNames.valueOf(key).name(), value);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static String get(PropNames name) {
    return propMap.get(name.name());
  }

}
