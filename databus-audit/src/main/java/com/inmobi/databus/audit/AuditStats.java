package com.inmobi.databus.audit;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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

/*
 * This class is responsible for launching multiple AuditStatsFeeder instances one per cluster
 */
public class AuditStats {
  public static final String CONF_FILE = "audit-feeder.properties";
  private static final String DATABUS_CONF_FILE_KEY = "feeder.databus.conf";
  private static final Log LOG = LogFactory.getLog(AuditStats.class);
  public final static MetricRegistry metrics = new MetricRegistry();

  private synchronized void start(List<AuditService> feeders) throws Exception {
    ClientConfig config = ClientConfig.loadFromClasspath(CONF_FILE);
    String databusConf = config.getString(DATABUS_CONF_FILE_KEY);
    DatabusConfigParser parser = new DatabusConfigParser(databusConf);
    DatabusConfig dataBusConfig = parser.getConfig();
    for (Entry<String, Cluster> cluster : dataBusConfig.getClusters()
        .entrySet()) {
      String rootDir = cluster.getValue().getRootDir();
      AuditService feeder = new AuditFeederService(cluster.getKey(), rootDir,
          config);
      feeders.add(feeder);
    }
    // start all feeders
    for (AuditService feeder : feeders) {
      LOG.info("starting feeder for cluster " + feeder.getServiceName());
      feeder.start();
    }

    startMetricsReporter(config);
  }

  private void join(List<AuditService> feeders) {
    for (AuditService feeder : feeders) {
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

  public synchronized void stop(List<AuditService> feeders) {

    try {
      LOG.info("Stopping Feeder...");
      for (AuditService feeder : feeders) {
        LOG.info("Stopping feeder  " + feeder.getServiceName());
        feeder.stop();
      }
      LOG.info("All feeders signalled to  stop");
    } catch (Exception e) {
      LOG.warn("Error in shutting down feeder", e);
    }

  }

  public static void main(String args[]) throws Exception {
    final AuditStats stats = new AuditStats();
    final List<AuditService> feeders = new ArrayList<AuditService>();
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
