package com.inmobi.databus.audit;

import com.inmobi.databus.audit.services.AuditRollUpService;
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

  private static final Log LOG = LogFactory.getLog(AuditStats.class);
  public final static MetricRegistry metrics = new MetricRegistry();

  final List<AuditDBService> dbServices = new ArrayList<AuditDBService>();
  private final ClientConfig config;
  private List<DatabusConfig> databusConfigList;
  private Map<String, Cluster> clusterMap;

  public AuditStats() throws Exception {
    config = ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE);
    config.set(MessagingConsumerConfig.hadoopConfigFileKey,
        "audit-core-site.xml");
    String databusConfFolder = config.getString(AuditDBConstants
        .DATABUS_CONF_FILE_KEY);
    loadConfigFiles(databusConfFolder);
    createClusterMap();
    for (Entry<String, Cluster> cluster : clusterMap.entrySet()) {
      String rootDir = cluster.getValue().getRootDir();
      AuditDBService feeder = new AuditFeederService(cluster.getKey(), rootDir,
          config);
      dbServices.add(feeder);
    }
    AuditDBService rollup = new AuditRollUpService(config);
    dbServices.add(rollup);
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

  private synchronized void start() throws Exception {
    // start all dbServices
    for (AuditDBService service : dbServices) {
      LOG.info("Starting service: " + service.getServiceName());
      service.start();
    }
    startMetricsReporter(config);
  }

  private void join() {
    for (AuditDBService service : dbServices) {
      service.join();
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

  public synchronized void stop() {

    try {
      LOG.info("Stopping all services...");
      for (AuditDBService service : dbServices) {
        LOG.info("Stopping service :" + service.getServiceName());
        service.stop();
      }
      LOG.info("All services signalled to stop");
    } catch (Exception e) {
      LOG.warn("Error in shutting down feeder and rollup services", e);
    }

  }

  public static void main(String args[]) throws Exception {
    final AuditStats stats = new AuditStats();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        stats.stop();
        stats.join();
        LOG.info("Finishing the shutdown hook");
      }
    });
    // TODO check if current table exist else create it.NOTE:This will be done
    // in next version

    try {
      stats.start();
      // wait for all dbServices to finish
      stats.join();
    } finally {
      stats.stop();
    }
  }
}
