package com.conduit.metrics;

import org.apache.commons.configuration.Configuration;

import com.conduit.metrics.context.factory.ContextFactory;
import com.conduit.metrics.context.impl.CompositeContext;

public class AbstractMetrics {

	private MetricsType type = null;
	private Configuration config = null;

	private CompositeContext allContext = null;

	/**
	 * Read the config and create the initParams. It would mainly contais the
	 * metrics implementation eg metrics.codahale and different reported to use
	 * eg Ganglia/console and each config eg gangia host /ganlia port etc
	 * 
	 * @throws Exception
	 */
	void init(Configuration config) throws Exception {
		InitializationUtil.initilizae(config);

		MetricsType metricType = (MetricsType) config.getProperty(ConfigNames.PRIVATE_METRICSTYPE_IMPL);
		this.type = metricType;
		CompositeContext allContext = ContextFactory.getCompositeContext(metricType, config);
		allContext.register();
		allContext.start();

	}

	public Configuration getConfig() {
		return config;
	}

	public MetricsType getType() {
		return type;
	}

	public AbstractMetrics(Configuration config) {
		this.config = config;
	}

	void stopAll() {
		this.allContext.stop();
	}

}
