package com.conduit.metrics.guage;

import org.apache.commons.configuration.Configuration;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.conduit.metrics.MetricsType;
import com.conduit.metrics.guage.met.impl.MetricsCounter;
import com.conduit.metrics.guage.met.impl.MetricsGuage;

public class GuagueFactory {

	public static Guage createLongGuage(MetricsType type, String name, Configuration config, final Number guageValue) {
		switch (type) {
		case CODAHALE:
			MetricRegistry metrics = (MetricRegistry) config.getProperty("registry");
			metrics.register(MetricRegistry.name("name"), new Gauge<Number>() {
				public Number getValue() {
					return guageValue;
				}

			});
			return new MetricsGuage(guageValue);

		default:

			return new MetricsGuage(guageValue);
		}

	}

	public static com.conduit.metrics.guage.Counter createCounter(MetricsType type, String name, Configuration config) {
		switch (type) {
		case CODAHALE:
			MetricRegistry metrics = (MetricRegistry) config.getProperty("registry");
			Counter pendingJobs = metrics.counter(name);
			return new MetricsCounter(pendingJobs);
		}

		return null;

	}
}
