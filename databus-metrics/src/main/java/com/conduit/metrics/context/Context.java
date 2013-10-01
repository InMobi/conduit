package com.conduit.metrics.context;

import org.apache.commons.configuration.Configuration;

public interface Context {

	void register() throws Exception;

	ContextType getType();

	void start();

	void stop();

	Configuration getConfigMap();

}
