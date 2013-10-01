package com.conduit.metrics.context.impl;


import org.apache.commons.configuration.Configuration;

import com.conduit.metrics.context.Context;
import com.conduit.metrics.context.ContextType;

public abstract class GangliaContext implements Context {

	private Configuration config;

	ContextType cType = ContextType.GANGLIA;

	public ContextType getType() {
		return cType;

	}

	public GangliaContext(Configuration config) {
		this.config = config;
	}

	abstract public String getHostName();

	abstract public int getPort();

	public Configuration getConfigMap() {
		return config;
	}

}
