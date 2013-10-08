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
		ReporterManager.create(registry, config);
		ReporterManager.startAll();

	}

	public static void stop() {
		ReporterManager.stopAll();

	}

	public static AbsoluteGuage registerAbsoluteGuage(String name, Number initalValue) {
		if (registry.getGauges().get(name) != null) {
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

	public static void registerGuage(String name, @SuppressWarnings("rawtypes") Gauge gaugeInst) {
		if (registry.getGauges().get(name) != null) {
			throw new RuntimeException("Guange with name " + name + " already exsits");
		}

		registry.register(name, gaugeInst);

	}

	public static Counter registerCounter(String name) {
		if (registry.getCounters().get(name) != null) {
			throw new RuntimeException("Counter with name " + name + " already exsits");
		}
		return registry.counter(name);
	}

}
