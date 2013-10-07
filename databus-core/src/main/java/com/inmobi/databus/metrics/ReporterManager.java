package com.inmobi.databus.metrics;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.ganglia.GangliaReporter;

public class ReporterManager {

	private final static String GANLIA = "ganlia";
	private final static String CONSOLE = "console";

	private final static String GANGLIA_SERVERNAME = "ganglia.serverName";
	private final static String GANGLIA_PORT = "ganglia.port";

	private final static Map<String, ScheduledReporter> reporterMap = new HashMap<String, ScheduledReporter>();

	public static void register(MetricRegistry registry, Configuration config) throws IOException {
		if (config.getBoolean(GANLIA, false)) {
			final GMetric ganglia = new GMetric(config.getString(GANGLIA_SERVERNAME), config.getInt(GANGLIA_PORT), UDPAddressingMode.MULTICAST, 1);
			GangliaReporter reporter = GangliaReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build(ganglia);
			reporterMap.put(GANLIA, reporter);
		}
		if (config.getBoolean(CONSOLE, false)) {
			ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
			reporterMap.put(CONSOLE, reporter);
		}

	}

	public static void start() {
		for (String eachReporterName : reporterMap.keySet()) {
			reporterMap.get(eachReporterName).start(1, TimeUnit.SECONDS);
		}

	}

	public static void stop() {

		for (String eachReporterName : reporterMap.keySet()) {
			reporterMap.get(eachReporterName).stop();
		}

	}

}
