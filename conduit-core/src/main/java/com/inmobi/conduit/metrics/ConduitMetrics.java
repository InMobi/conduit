package com.inmobi.conduit.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.ganglia.GangliaReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Metrics manager class. Will use codahale metrics as the implementation.
 * Contains functions to register Gauges/Counters and start/stop reporters
 */
public class ConduitMetrics {

  private final static String GANGLIA = "com.inmobi.conduit.metrics.ganglia";
  private final static String CONSOLE = "com.inmobi.conduit.metrics.console";
  private final static String GANGLIA_SERVERNAME = "com.inmobi.conduit.metrics.ganglia.serverName";
  private final static String GANGLIA_PORT = "com.inmobi.conduit.metrics.ganglia.port";
  private final static String REPORTING_PERIOD = "com.inmobi.conduit.metrics.period";
  private final static String IS_ENABLED_PROPERTY="com.inmobi.conduit.metrics.enabled";
  private final static String LOCAL_SERVICE = "LocalStreamService";
  private final static String MERGED_SERVICE = "MergedStreamService";
  private final static String MIRROR_SERVICE = "MirrorStreamService";
  private final static String PURGER_SERVICE = "DataPurgerService";
  private final static String SLIDING_WINDOW_TIME ="com.inmobi.conduit.metrics.slidingwindowtime";


  private final static Map<String, ScheduledReporter> reporterMap =
      new HashMap<String, ScheduledReporter>();
  private static final Log LOG = LogFactory.getLog(ConduitMetrics.class);

  private static boolean isEnabled =false;
  private static int timeBetweenPolls = 10;
  private static int slidingwindowtime= 0;
  private static MetricRegistry registry;

  /*
   *The three level cache. every counter would be accessed as 
   *(Service,counterType,(Context)StreamName/ServiceName) => counter
   *Each level is maintained by a hashMap, The highest level is the serviceLevel
   *which is created on init(), rest is created lazily
   */
  private final static Map<String, Map<String, Map<String, Metric>>> threeLevelCache =
      new HashMap<String, Map<String, Map<String, Metric>>>();

  /**
   * Will create reporters based on config
   */
  public static void init(Properties config) throws IOException {

    if(config.getProperty(IS_ENABLED_PROPERTY,"false").equalsIgnoreCase("true")){
      isEnabled=true;
      threeLevelCache.put(LOCAL_SERVICE, new HashMap<String , Map<String, Metric>>());
      threeLevelCache.put(MIRROR_SERVICE, new HashMap<String , Map<String, Metric>>());
      threeLevelCache.put(MERGED_SERVICE, new HashMap<String , Map<String, Metric>>());
      threeLevelCache.put(PURGER_SERVICE, new HashMap<String , Map<String, Metric>>());
      registry= new MetricRegistry();
    }else{
      return;
    }
    timeBetweenPolls = Integer.parseInt(config.getProperty(REPORTING_PERIOD , "10"));
    slidingwindowtime = Integer.parseInt(config.getProperty(REPORTING_PERIOD, "10"));

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

  public static AbsoluteGauge registerAbsoluteGauge(String serviceName,
                                                    String counterType,
                                                    String context) {
    final String metricName = createName(serviceName, counterType, context);
    AbsoluteGauge metric = registerAbsoluteGauge(metricName, 0);
    if (metric != null) {
      addToCache(serviceName, counterType, context, metric);
    }
    return metric;
  }

  /**
   * Create an AbsoluteGauge
   */
  public static AbsoluteGauge registerAbsoluteGauge(String name,
      Number initalValue) {
    if(!isEnabled){
      LOG.warn("metrics not enabled");
      return null;
    }
    if (registry.getGauges().get(name) != null) {
      LOG.warn("Gauge with name " + name + " already exsits");
      return null;
    }

    final AbsoluteGauge codahalegaugeInst = new AbsoluteGauge(initalValue);
    registry.register(name, codahalegaugeInst);
    return codahalegaugeInst;
  }

  public static void updateAbsoluteGauge(String serviceName,
                                         String counterType,
                                         String context, Number value) {
    LOG.info("absolute gauge value " + serviceName + "." + counterType + "." +
        context +"= " + value);
    AbsoluteGauge c = getMetric(serviceName, counterType, context);
    if (c != null) {
      c.setValue(value);
    }
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
   */
  synchronized public static Counter registerCounter(String serviceName,
      String counterType, String context) {
    if(!isEnabled){
      LOG.warn("metrics not enabled");
      return null;
    }
    if (registry.getCounters().get(createName(serviceName , counterType , context)) != null) {
      LOG.warn("Counter with name " + createName(serviceName , counterType ,
           context )+ " already exsits");
      return null;
    }
    Counter counterInst = registry.counter(createName(serviceName , counterType ,
         context));
    addToCache(serviceName, counterType, context, counterInst);
    return counterInst;
  }
  
  private static String createName(String serviceName,
      String counterType, String context){
	  return serviceName + "." + counterType + "."
		        + context;
  }

  /**
   * Get a counter from the cache
   */
	
  @SuppressWarnings("unchecked")
  public static <T extends Metric> T getMetric(String serviceName,
			String counterType, String context) {
		if (!isEnabled) {
			LOG.warn("metrics not enabled");
			return null;
		}
		Metric c = getFromCache(serviceName, counterType, context);
		try {
			return (T) c;
		} catch (Exception e) {
			LOG.warn("Not able to convert to type " + serviceName + "."
					+ counterType + "." + context + " ->" + e.getMessage());
			return null;
		}

	}
  
  
	private static void addToCache(String serviceName, String counterType,
			String context, Metric any) {

		Map<String, Map<String, Metric>> serviceLevelCache = threeLevelCache
				.get(serviceName);
		Map<String, Metric> counterTypeLevel = serviceLevelCache
				.get(counterType);
		if (counterTypeLevel == null) {
			counterTypeLevel = new HashMap<String, Metric>();
			serviceLevelCache.put(counterType, counterTypeLevel);
		}
		
		counterTypeLevel.put(context, any);
		

	}
	
	
	private static Metric getFromCache(String serviceName, String counterType,
			String context) {

		Map<String, Metric> counterTypeLevel = threeLevelCache.get(serviceName)
				.get(counterType);
		if (counterTypeLevel == null) {
			LOG.info("metric does not exist:" + serviceName + "." + counterType
					+ "." + context);
			return null;
		}
		Metric c = counterTypeLevel.get(context);
		if (c == null) {
			LOG.info("metric does not exist:" + serviceName + "." + counterType
					+ "." + context);
			return null;
		}
		return c;
	}

  public static void incCounter(String serviceName, String counterType,
      String context ,long value){
    Counter c = getMetric(serviceName, counterType , context);
    if(c!=null){
      c.inc(value);
    }
  }
  
  public static SlidingTimeWindowGauge registerSlidingWindowGauge(String serviceName,
	      String counterType, String context) {
	final String metricsName =createName(serviceName, counterType, context);  
    if(!isEnabled){
      LOG.warn("metrics not enabled");
      return null;
    }
    if (registry.getGauges().get(metricsName) != null) {
      LOG.warn("SlidingTimeWindowGuage Gauge with name " + metricsName + " already exsits");
      return null;
    }

    final SlidingTimeWindowGauge codahalegaugeInst = new SlidingTimeWindowGauge(slidingwindowtime, TimeUnit.SECONDS);
    registry.register(metricsName, codahalegaugeInst);
    addToCache(serviceName, counterType, context, codahalegaugeInst);
    return codahalegaugeInst;

  }
  
  
	public static void updateSWGuage(String serviceName, String counterType,
			String context, long value) {
	  LOG.info("sliding window  value " + serviceName + "." + counterType + "." + context +"= " + value);
		SlidingTimeWindowGauge c = getMetric(serviceName, counterType, context);
		if (c != null) {
			c.setValue(value);
		}
	}
}
