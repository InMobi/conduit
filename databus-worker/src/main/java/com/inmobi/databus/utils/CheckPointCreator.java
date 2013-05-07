package com.inmobi.databus.utils;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;

public class CheckPointCreator {

  private final DatabusConfig config;
  private final String srcCluster;
  private final String destnCluster;
  private final String stream;

  public CheckPointCreator(DatabusConfig config, String sourceCluster,
      String destinationCluster, String stream) {
    this.config = config;
    srcCluster = sourceCluster;
    destnCluster = destinationCluster;
    this.stream = stream;
  }

  public void createCheckPoint(){
    Cluster destinationCluster=config.getClusters().get(destnCluster);
    if (srcCluster == null) {// no src clusters provided;create checkpoint for
                             // all src clusters

    }
  }

}
