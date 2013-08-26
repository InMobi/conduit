package com.inmobi.databus.visualization.server;

public class DataServiceManagerTest extends DataServiceManager {
  private static DataServiceManagerTest instance = null;

  private DataServiceManagerTest(String xmlPath,
                                 String visualizationPropPath,
                                 String feederPropPath) {
    super(false, visualizationPropPath, feederPropPath);
    initConfig(xmlPath);
  }

  public static DataServiceManagerTest get(String xmlpath,
                                           String visualizationPropPath,
                                           String feederPropPath) {
    instance = new DataServiceManagerTest(xmlpath, visualizationPropPath,
        feederPropPath);
    return instance;
  }
}
