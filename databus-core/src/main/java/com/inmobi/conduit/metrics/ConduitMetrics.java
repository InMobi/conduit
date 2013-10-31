package com.inmobi.conduit.metrics;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.ganglia.GangliaReporter;

/**
 * Metrics manager class. Will use codahale metrics as the implementation.
 * Contains functions to register Gauges/Counters and start/stop reporters
 * 
 * 
 */
public class ConduitMetrics {

  private static final Log LOG = LogFactory.getLog(ConduitMetrics.class);

  private static MetricRegistry registry;

  private final static String GANGLIA = "com.inmobi.databus.metrics.ganglia";
  private final static String CONSOLE = "com.inmobi.databus.metrics.console";
  private final static String GANGLIA_SERVERNAME = "com.inmobi.databus.metrics.ganglia.serverName";
  private final static String GANGLIA_PORT = "com.inmobi.databus.metrics.ganglia.port";
  private final static String REPORTING_PERIOD = "com.inmobi.databus.metrics.period";
  private final static String IS_ENABLED_PROPERTY="com.inmobi.databus.metrics.enabled";


  private final static Map<String, ScheduledReporter> reporterMap =
      new HashMap<String, ScheduledReporter>();
  /*
   *The three level cache. every counter would be accessed as 
   *(Service,counterType,(Context)StreamName/ServiceName) => counter
   *Each level is maintained by a hashMap, The highest level is the serviceLevel
   *which is created on init(), rest is created lazily
   */
  private final static Map<String, Map<String, Map<String, Counter>>> threeLevelCache =
      new HashMap<String, Map<String, Map<String, Counter>>>();

  private static boolean isEnabled =false;
  private static int timeBetweenPolls = 10;

  /**
   * Will create reporters based on config
   * 
   * @param config
   * @throws IOException
   */
  public static void init(Properties config) throws IOException {

    if(config.getProperty(IS_ENABLED_PROPERTY,"false").equalsIgnoreCase("true")){
      isEnabled=true;
      threeLevelCache.put("LocalStreamService", new HashMap<String , Map<String, Counter>>());
      threeLevelCache.put("MirrorStreamService", new HashMap<String , Map<String, Counter>>());
      threeLevelCache.put("MergedStreamService", new HashMap<String , Map<String, Counter>>());
      threeLevelCache.put("DataPurgerService", new HashMap<String , Map<String, Counter>>());
      registry= new MetricRegistry();
    }else{
      return;
    }
    timeBetweenPolls = Integer.parseInt(config.getProperty(REPORTING_PERIOD , "10"));

    if (config.getProperty(GANGLIA, "false").equalsIgnoreCase("true")) {
      final GMetric ganglia = new GMetric(config.getProperty(GANGLIA_SERVERNAME),
          Integer.parseInt(config.getProperty(GANGLIA_PORT)),
          UDPAddressingMode.MULTICAST, 1);
      GangliaReporter reporter = GangliaReporter.forRegistry(registry)
          .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
          .build(ganglia);
      reporterMap.put(GANGLIA, reporter);
      LOG.info("Ganglia Reporter registered");
    }
    if (config.getProperty(CONSOLE, "false").equalsIgnoreCase("true")) {
      ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
          .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
          .build();
      reporterMap.put(CONSOLE, reporter);
      LOG.info("Console reporter registered");
    }

    if (reporterMap.size() == 0) {
      LOG.warn("No reporter registered");
    }

  }

  /**
   * Will start all reporters
   */
  public static void startAll() {
    if(!isEnabled){
      LOG.warn("metrics not enabled");
      return ;
    }
    if (reporterMap.size() == 0) {
      LOG.warn("No reporter registered , nothing to start");
      return;
    }
    for (String eachReporterName : reporterMap.keySet()) {
      reporterMap.get(eachReporterName).start(timeBetweenPolls, TimeUnit.SECONDS);
    }
    LOG.info("started all reporters");
  }

  /**
   * Will stop all reporters
   */
  public static void stopAll() {
    if(!isEnabled){
      LOG.warn("metrics not enabled");
      return ;
    }
    if (reporterMap.size() == 0) {
      LOG.warn("No reporter registered , nothing to stop");
    }
    for (String eachReporterName : reporterMap.keySet()) {
      reporterMap.get(eachReporterName).stop();
    }
    registry = null;
    isEnabled=false;
    reporterMap.clear();
    threeLevelCache.clear();

  }

  /**
   * Create a Gauge where a value can be set by using the AbsoluteGauge
   * methods
   * 
   * @param name
   * @param initalValue
   * @return
   */
  public static AbsoluteGauge registerAbsoluteGauge(String name, Number initalValue) {
    if(!isEnabled){
      LOG.warn("metrics not enabled");
      return null;
    }
    if (registry.getGauges().get(name) != null) {
      LOG.warn("Gauge with name " + name + " already exsits");
      return null;
    }

    final AbsoluteGauge codahalegaugeInst = new AbsoluteGauge(initalValue);
    Gauge<Number> gauge = new Gauge<Number>() {
      public Number getValue() {
        return codahalegaugeInst.getValue();
      }
    };

    registry.register(name, gauge);

    return codahalegaugeInst;

  }

  /*
   * Register a codahale.metrics type Gauge.
   */
  @SuppressWarnings("rawtypes")
  public static Gauge registerGauge(String name, Gauge gaugeInst) {
    if(!isEnabled){
      LOG.warn("metrics not enabled");
      return null;
    }
    if (registry.getGauges().get(name) != null) {
      LOG.warn("Gauge with name " + name + " already exsits");
      return null;

    }

    return registry.register(name, gaugeInst);

  }



  /**
   * Register a counter
   * 
   * @param name
   * @return
   */
  synchronized public static Counter registerCounter(String serviceName, String counterType, String context) {
    if(!isEnabled){
      LOG.warn("metrics not enabled");
      return null;
    }
    if (registry.getCounters().get(serviceName+"."+counterType+"."+context) != null) {
      LOG.warn("Counter with name " + serviceName + counterType + context + " already exsits");
      return null;
    }
    Map<String, Map<String, Counter>> serviceLevelCache = threeLevelCache.get(serviceName);
    Map<String, Counter> counterTypeLevel = serviceLevelCache.get(counterType);
    if(counterTypeLevel == null){
      counterTypeLevel = new HashMap<String , Counter>();
      serviceLevelCache.put(counterType, counterTypeLevel);
    }
    Counter counterInst = registry.counter(serviceName+"."+counterType+"."+context);
    counterTypeLevel.put(context, counterInst);
    return counterInst;
  }

  /**
   * Get a counter from the cache
   */
  public static Counter getCounter(String serviceName, String counterType, String context) {
    if(!isEnabled){
      LOG.warn("metrics not enabled");
      return null;
    }
    Map<String, Map<String, Counter>> serviceLevel = threeLevelCache.get(serviceName);

    Map<String, Counter> counterTypeLevel = serviceLevel.get(counterType);
    if(counterTypeLevel == null){
      LOG.info("counter does not exist:" );
      return null;
    }
    Counter c = counterTypeLevel.get(context);
    if(c == null){
      LOG.info("counter does not exist:" + serviceName + "." +counterType + "." + context);
      return null;
    }
    return c;
  }

  public static void incCounter(String serviceName, String counterType, String context ,long value){
    Counter c = getCounter(serviceName, counterType , context);
    if(c!=null){
      c.inc(value);
    }
  }

}
