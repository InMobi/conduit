package com.inmobi.databus.visualization.server;

import java.io.File;
import java.io.FileInputStream;
import java.util.Hashtable;
import java.util.Properties;

public class VisualizationProperties {

  private static Hashtable<String, String> propMap =
      new Hashtable<String, String>();

  public VisualizationProperties(String propertiesFilePath) {
    if (propertiesFilePath == null || propertiesFilePath.length() == 0) {
      loadPropMap(ServerConstants.VISUALIZATION_PROPERTIES_DEFAULT_PATH);
    } else {
      loadPropMap(propertiesFilePath);
    }
  }

  private void loadPropMap(String filePath) {
    try {
      Properties p = new Properties();
      p.load(new FileInputStream(new File(filePath)));
      propMap.put(ServerConstants.DATABUS_XML_PATH, p.get("databus.xml.path")
          .toString());
      propMap.put(ServerConstants.PERCENTILE_STRING,
          p.get("percentile.string").toString());
      propMap.put(ServerConstants.PUBLISHER_SLA, p.get("publisher.sla")
          .toString());
      propMap.put(ServerConstants.AGENT_SLA, p.get("agent.sla").toString());
      propMap.put(ServerConstants.VIP_SLA, p.get("vip.sla").toString());
      propMap.put(ServerConstants.COLLECTOR_SLA, p.get("collector.sla")
          .toString());
      propMap.put(ServerConstants.HDFS_SLA, p.get("hdfs.sla").toString());
      propMap.put(ServerConstants.PERCENTILE_FOR_SLA,
          p.get("percentile.for.sla").toString());
      propMap.put(ServerConstants.PERCENTAGE_FOR_LOSS,
          p.get("percentage.for.loss").toString());
      propMap.put(ServerConstants.PERCENTAGE_FOR_WARN,
          p.get("percentage.for.warn").toString());
      propMap.put(ServerConstants.MAX_START_TIME, p.get("max.start.time")
          .toString());
      propMap.put(ServerConstants.MAX_TIME_RANGE_INTERVAL_IN_HOURS,
          p.get("max.time.range.interval.in.hours").toString());
      propMap.put(ServerConstants.LOSS_WARN_THRESHOLD_DIFF_IN_MINS,
          p.get("loss.warn.threshold.diff.in.mins").toString());
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Error while initializing propMap:" + e.getMessage());
    }
  }

  public boolean set(String key, String value) {
    try {
      propMap.put(key, value);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public String get(String name) {
    return propMap.get(name);
  }

}
