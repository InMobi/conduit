package com.inmobi.conduit.validator;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StreamsValidator {
  private static final Log LOG = LogFactory.getLog(StreamsValidator.class);
  private ConduitConfig conduitConfig = null;
  private Set<String> streams = new HashSet<String>();
  private Set<String> modes = new HashSet<String>();
  private Set<String> clusters = new HashSet<String>();
  private Date startTime;
  private Date stopTime;
  private int numThreads;

  public StreamsValidator(ConduitConfig conduitConfig, String streamNames,
      String modeNames, String clusterNames, Date startTime, Date stopTime,
      int numThreads) {
    this.conduitConfig = conduitConfig;

    if (streamNames == null || streamNames.isEmpty()) {
      // add ALL streams from conduit config
      for (String stream : conduitConfig.getSourceStreams().keySet()) {
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
      //add ALL clusters from conduit config
      for (String cluster : conduitConfig.getClusters().keySet()) {
        clusters.add(cluster);
      }
    } else {
      for (String cluster : clusterNames.split(",")) {
        clusters.add(cluster);
      }
    }
    this.startTime = startTime;
    this.stopTime = stopTime;
    this.numThreads = numThreads;
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
      Cluster cluster = conduitConfig.getClusters().get(clusterName);
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
      Cluster cluster = conduitConfig.getClusters().get(clusterName);
      if (!cluster.getPrimaryDestinationStreams().contains(stream)) {
        System.out.println("ERROR: The stream [" + stream +
            "] is not running in [MERGE] mode on cluster [" + clusterName + "]");
        continue;
      }
      
      MergedStreamValidator mergeValidator = new MergedStreamValidator(
          conduitConfig, stream, clusterName, fix, startTime, stopTime,
          numThreads);
      mergeValidator.execute();
    }
  }

  private void validateMirrorStream(String stream, boolean fix) throws Exception {
    // for each cluster, check whether stream runs in mirror mode
    for (String clusterName: clusters) {
      Cluster cluster = conduitConfig.getClusters().get(clusterName);
      if (!cluster.getMirroredStreams().contains(stream)) {
        System.out.println("ERROR: The stream [" + stream +
            "] is not running in [MIRROR] mode on cluster [" + clusterName + "]");
        continue;
      }
      // add start time and stop time
      MirrorStreamValidator mirrorValidator = new MirrorStreamValidator(
          conduitConfig, stream, clusterName, fix, startTime, stopTime,
          numThreads);
      mirrorValidator.execute();
    }
  }
}
