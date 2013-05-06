package com.inmobi.databus.validator;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;

public class StreamsValidator {
  private static final Log LOG = LogFactory.getLog(StreamsValidator.class);
  private DatabusConfig databusConfig = null;
  private List<String> streams = new ArrayList<String>();
  private List<String> modes = new ArrayList<String>();
  private List<String> clusters = new ArrayList<String>();
  private Date startTime;
  private Date stopTime;

  public StreamsValidator(DatabusConfig databusConfig, String streamNames, 
      String modeNames, String clusterNames, Date startTime, Date stopTime) {
    this.databusConfig = databusConfig;

    if (streamNames == null || streamNames.isEmpty()) {
      // add ALL streams from databus config
      for (String stream : databusConfig.getSourceStreams().keySet()) {
        streams.add(stream);
      }
    } else {
      for (String stream : streamNames.split(",")) {
        streams.add(stream);
      }
    }

    if (modeNames == null || modeNames.isEmpty()) {
      // add ALL modes for stream validation
      modes.add("local");
      modes.add("merge");
      modes.add("mirror");
    } else {
      for (String mode : modeNames.split(",")) {
        modes.add(mode.toLowerCase());
      }
    }

    if (clusterNames == null || clusterNames.isEmpty()) {
      //add ALL clusters from databus config
      for (String cluster : databusConfig.getClusters().keySet()) {
        clusters.add(cluster);
      }
    } else {
      for (String cluster : clusterNames.split(",")) {
        clusters.add(cluster);
      }
    }
    // start time
    this.startTime = startTime;
    // stop time
    this.stopTime = stopTime;
  }

  public void validateStreams(boolean fix) throws Exception {
    for (String stream : streams) {
      // run the stream validation in the following order of modes:
      // LOCAL, MERGE, MIRROR
      if (modes.contains("local")) {
        validateLocalStream(stream, fix);
      }
      if (modes.contains("merge")) {
        validateMergeStream(stream, fix);
      }
      if (modes.contains("mirror")) {
        validateMirrorStream(stream, fix);
      }
    }
  }

  private void validateLocalStream(String stream, boolean fix) throws Exception {
    // for each cluster, check whether stream runs in local mode
    for (String clusterName: clusters) {
      Cluster cluster = databusConfig.getClusters().get(clusterName);
      if (!cluster.getSourceStreams().contains(stream)) {
        System.out.println("ERROR: The stream [" + stream +
            "] is not running in [LOCAL] mode on cluster [" + clusterName + "]");
        continue;
      }
    }
      
      // TODO: perform local stream validation
  }

  private void validateMergeStream(String stream, boolean fix) throws Exception {
    // for each cluster, check whether stream runs in merge mode
    for (String clusterName: clusters) {
      Cluster cluster = databusConfig.getClusters().get(clusterName);
      if (!cluster.getPrimaryDestinationStreams().contains(stream)) {
        System.out.println("ERROR: The stream [" + stream +
            "] is not running in [MERGE] mode on cluster [" + clusterName + "]");
        continue;
      }
      
      // TODO: perform merge stream validation
    }
  }

  private void validateMirrorStream(String stream, boolean fix) throws Exception {
    // for each cluster, check whether stream runs in mirror mode
    for (String clusterName: clusters) {
      Cluster cluster = databusConfig.getClusters().get(clusterName);
      if (!cluster.getMirroredStreams().contains(stream)) {
        System.out.println("ERROR: The stream [" + stream +
            "] is not running in [MIRROR] mode on cluster [" + clusterName + "]");
        continue;
      }
      // add start time and stop time
      MirrorStreamValidator mirrorValidator = new MirrorStreamValidator(databusConfig, 
          stream, clusterName, fix);
      mirrorValidator.execute();
    }
  }
}
