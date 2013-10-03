package com.conduit.metrics.guage;

import org.apache.commons.configuration.Configuration;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.conduit.metrics.MetricsType;
import com.conduit.metrics.guage.met.impl.CodahaleCounter;
import com.conduit.metrics.guage.met.impl.CodahaleGuage;
import com.conduit.metrics.util.ConfigNames;
/**
 * To create guage/counter/etc
 * we can more measure like histogram etc
 * @author samar.kumar
 *
 */
public class MeasureFactory {

	public static Guage createLongGuage(MetricsType type, String name, Configuration config, final Number guageValue) {
		switch (type) {
		case CODAHALE:
			MetricRegistry metrics = (MetricRegistry) config.getProperty(ConfigNames.CODAHALE_REGISTRY);
			metrics.register(MetricRegistry.name(name), new Gauge<Number>() {
				public Number getValue() {
					return guageValue;
				}

			});
			return new CodahaleGuage(guageValue);

		default:
			throw new RuntimeException("Metrics Implementation type unknow");
		}

	}

	public static com.conduit.metrics.guage.Counter createCounter(MetricsType type, String name, Configuration config) {
		switch (type) {
		case CODAHALE:
			MetricRegistry metrics = (MetricRegistry) config.getProperty(ConfigNames.CODAHALE_REGISTRY);
			Counter pendingJobs = metrics.counter(name);
			return new CodahaleCounter(pendingJobs);

		default:
			throw new RuntimeException("Metrics Implementation type unknow");
		}

	}
}
