package com.inmobi.databus.metrics;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.ganglia.GangliaReporter;
/**
 * Metrics manager class. Will use codahale metrics as the implementation. Contains functions to register Gauges/Counters start/stop reporters
 * @author samar.kumar
 *
 */
public class ConduitMetrics {

	private static final Log LOG = LogFactory.getLog(ConduitMetrics.class);

	private final static MetricRegistry registry;

	private final static String GANGLIA = "ganlia";
	private final static String CONSOLE = "console";

	private final static String GANGLIA_SERVERNAME = "ganglia.serverName";
	private final static String GANGLIA_PORT = "ganglia.port";

	private final static Map<String, ScheduledReporter> reporterMap = new HashMap<String, ScheduledReporter>();

	static {
		registry = new MetricRegistry();
	}

	public static void init(Configuration config) throws IOException {

		if (config.getBoolean(GANGLIA, false)) {
			final GMetric ganglia = new GMetric(config.getString(GANGLIA_SERVERNAME), config.getInt(GANGLIA_PORT), UDPAddressingMode.MULTICAST, 1);
			GangliaReporter reporter = GangliaReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build(ganglia);
			reporterMap.put(GANGLIA, reporter);
			LOG.info("Ganglia Reporter registered");
		}
		if (config.getBoolean(CONSOLE, false)) {
			ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
			reporterMap.put(CONSOLE, reporter);
			LOG.info("Console reporter registered");
		}

		if (reporterMap.size() == 0) {
			LOG.error("No reporter registered");
		}

	}

	public static void startAll() {
		if (reporterMap.size() == 0) {
			LOG.error("No reporter registered , nothing to start");
		}
		for (String eachReporterName : reporterMap.keySet()) {
			reporterMap.get(eachReporterName).start(1, TimeUnit.SECONDS);
		}
	}

	public static void stopAll() {

		if (reporterMap.size() == 0) {
			LOG.error("No reporter registered , nothing to stop");
		}

		for (String eachReporterName : reporterMap.keySet()) {
			reporterMap.get(eachReporterName).stop();
		}

	}

	public static AbsoluteGuage registerAbsoluteGuage(String name, Number initalValue) {
		if (registry.getGauges().get(name) != null) {
			LOG.error("Guange with name " + name + " already exsits");
			return null;
		}

		final AbsoluteGuage codahaleguageInst = new AbsoluteGuage(initalValue);
		Gauge<Number> guage = new Gauge<Number>() {
			public Number getValue() {
				return codahaleguageInst.getValue();
			}
		};

		registry.register(name, guage);

		return codahaleguageInst;

	}

	@SuppressWarnings("rawtypes")
	public static Gauge registerGuage(String name, Gauge gaugeInst) {
		if (registry.getGauges().get(name) != null) {
			LOG.error("Guange with name " + name + " already exsits");
			return null;

		}

		return registry.register(name, gaugeInst);

	}

	public static Counter registerCounter(String name) {
		if (registry.getCounters().get(name) != null) {
			LOG.error("Counter with name " + name + " already exsits");
			return null;
		}
		return registry.counter(name);
	}

}
