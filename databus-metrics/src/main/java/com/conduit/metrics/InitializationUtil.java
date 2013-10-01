package com.conduit.metrics;

import org.apache.commons.configuration.Configuration;

public class InitializationUtil {


	public static Configuration initilizae(Configuration config) {
		String metricsImplType = config.getString("metricsType.impl");

		MetricsType type = MetricsType.valueOf(metricsImplType);
		config.setProperty("private.metricsType.impl", type);
		switch (type) {
		case Metrics:
			//set and check specific to metrics framework
			return config;
		default:
			throw new RuntimeException("Metrics Type Not supported");
		}
	}
}
