package com.conduit.metrics.util;

import org.apache.commons.configuration.Configuration;

import com.conduit.metrics.MetricsType;

public class InitializationUtil {

	public static Configuration initilizae(Configuration config) {
		String metricsImplType = config.getString("metricsType.impl");

		MetricsType type = MetricsType.valueOf(metricsImplType);
		config.setProperty(ConfigNames.PRIVATE_METRICSTYPE_IMPL, type);
		switch (type) {
		case CODAHALE:
			// set and checks specific to metrics framework
			return config;
		default:
			throw new RuntimeException("Metrics Type Not supported");
		}
	}
}
