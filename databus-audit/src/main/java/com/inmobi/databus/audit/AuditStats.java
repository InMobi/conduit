package com.inmobi.databus.audit;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.audit.services.AuditFeederService;
import com.inmobi.databus.audit.util.AuditDBConstants;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.databus.MessagingConsumerConfig;

/*
 * This class is responsible for launching multiple AuditStatsFeeder instances one per cluster
 */
public class AuditStats {
  public static final String CONF_FILE = "audit-feeder.properties";
  private static final String DATABUS_CONF_FILE_KEY = "feeder.databus.conf";
  private static final Log LOG = LogFactory.getLog(AuditStats.class);
  public final static MetricRegistry metrics = new MetricRegistry();
  private final ClientConfig config;
  private List<DatabusConfig> databusConfigList;
  private Map<String, Cluster> clusterMap;

  public AuditStats(List<AuditDBService> feeders) throws Exception {
    config = ClientConfig.loadFromClasspath(CONF_FILE);
    config.set(MessagingConsumerConfig.hadoopConfigFileKey,
        "audit-core-site.xml");
    String databusConfFolder = config.getString(DATABUS_CONF_FILE_KEY);
    loadConfigFiles(databusConfFolder);
    createClusterMap();
    for (Entry<String, Cluster> cluster : clusterMap.entrySet()) {
      String rootDir = cluster.getValue().getRootDir();
      AuditDBService feeder = new AuditFeederService(cluster.getKey(), rootDir,
          config);
      feeders.add(feeder);
    }
  }

  private void createClusterMap() {
    clusterMap = new HashMap<String, Cluster>();
    for (DatabusConfig dataBusConfig : databusConfigList) {
      clusterMap.putAll(dataBusConfig.getClusters());
    }
  }

  private void loadConfigFiles(String databusConfFolder) {
    File folder = new File(databusConfFolder);
    File[] xmlFiles = folder.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        if (file.getName().toLowerCase().endsWith(".xml")) {
          return true;
        }
        return false;
      }
    });
    LOG.info("Databus xmls included in the conf folder:");
    databusConfigList = new ArrayList<DatabusConfig>();
    for (File file : xmlFiles) {
      String fullPath = file.getAbsolutePath();
      LOG.info("File:"+fullPath);
      try {
        DatabusConfigParser parser = new DatabusConfigParser(fullPath);
        databusConfigList.add(parser.getConfig());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private synchronized void start(List<AuditDBService> feeders) throws Exception {
    // start all feeders
    for (AuditDBService feeder : feeders) {
      LOG.info("starting feeder for cluster " + feeder.getServiceName());
      feeder.start();
    }

    startMetricsReporter(config);
  }

  private void join(List<AuditDBService> feeders) {
    for (AuditDBService feeder : feeders) {
      feeder.join();
    }
  }

  private void startMetricsReporter(ClientConfig config) {
    String gangliaHost = config.getString(AuditDBConstants.GANGLIA_HOST);
    int gangliaPort = config.getInteger(AuditDBConstants.GANGLIA_PORT, 8649);
    if (gangliaHost != null) {
      GMetric ganglia;
      try {
        ganglia = new GMetric(gangliaHost, gangliaPort,
            UDPAddressingMode.MULTICAST, 1);
        GangliaReporter gangliaReporter = GangliaReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build(ganglia);
        gangliaReporter.start(1, TimeUnit.MINUTES);
      } catch (IOException e) {
        LOG.error("Cannot start ganglia reporter", e);
      }
    }
    String csvDir = config.getString(AuditDBConstants.CSV_REPORT_DIR, "/tmp");
    CsvReporter csvreporter = CsvReporter.forRegistry(metrics)
        .formatFor(Locale.US).convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS).build(new File(csvDir));
    csvreporter.start(1, TimeUnit.MINUTES);

  }

  public synchronized void stop(List<AuditDBService> feeders) {

    try {
      LOG.info("Stopping Feeder...");
      for (AuditDBService feeder : feeders) {
        LOG.info("Stopping feeder  " + feeder.getServiceName());
        feeder.stop();
      }
      LOG.info("All feeders signalled to  stop");
    } catch (Exception e) {
      LOG.warn("Error in shutting down feeder", e);
    }

  }

  public static void main(String args[]) throws Exception {

    final List<AuditDBService> feeders = new ArrayList<AuditDBService>();
    final AuditStats stats = new AuditStats(feeders);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        stats.stop(feeders);
        stats.join(feeders);
        LOG.info("Finishing the shutdown hook");
      }
    });
    // TODO check if current table exist else create it.NOTE:This will be done
    // in next version

    try {
      stats.start(feeders);
      // wait for all feeders to finish
      stats.join(feeders);
    } finally {
      stats.stop(feeders);
    }

  }
}
