package com.inmobi.databus.visualization.server;

public class DataServiceManagerTest extends DataServiceManager {
  private static DataServiceManagerTest instance = null;

  private DataServiceManagerTest(String folderPath) {
    super(false);
    initConfig(folderPath);
  }

  public static DataServiceManagerTest get(String path, boolean reset) {
    if (instance == null || reset) {
      instance = new DataServiceManagerTest(path);
    }
    return instance;
  }

  public void reset() {
    instance = null;
  }
}
