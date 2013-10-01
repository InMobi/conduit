package com.conduit.metrics.context.impl;

import org.apache.commons.configuration.Configuration;

import com.conduit.metrics.context.Context;
import com.conduit.metrics.context.ContextType;

public abstract class ConsoleContext implements Context {
	ContextType cType = ContextType.CONSOLE;
	Configuration config;

	public ConsoleContext(Configuration config) {
		this.config = config;
	}

	public Configuration getConfigMap() {
		return config;
	}

	public ContextType getType() {
		return cType;

	}

}
