package com.inmobi.databus.metrics;

import java.io.IOException;
import org.apache.commons.configuration.Configuration;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

public class ConduitMetrics {

	private final static MetricRegistry registry;

	static {
		registry = new MetricRegistry();
	}

	public static void init(Configuration config) throws IOException {
		ReporterManager.register(registry, config);
		ReporterManager.start();

	}

	public static void stop() {

	}

	public static AbsoluteGuage createAbsoluteGuage(String name, Number initalValue) {
		if (registry.getGauges().get("name") != null) {
			throw new RuntimeException("Guange with name " + name + " already exsits");
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

	public static void createGuage(String name, Gauge guageInst) {
		if (registry.getGauges().get("name") != null) {
			throw new RuntimeException("Guange with name " + name + " already exsits");
		}

		registry.register(name, guageInst);

	}

	public static Counter createCounter(String name) {
		if (registry.getCounters().get("name") != null) {
			throw new RuntimeException("Counter with name " + name + " already exsits");
		}
		return registry.counter(name);
	}

}
