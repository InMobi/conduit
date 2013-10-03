package com.conduit.metrics.context.met.impl;

import org.apache.commons.configuration.Configuration;

import com.codahale.metrics.MetricRegistry;
import com.conduit.metrics.context.Context;
import com.conduit.metrics.context.impl.CompositeContext;
import com.conduit.metrics.util.ConfigNames;

public class CodahaleCompositeContext extends CompositeContext {

	public CodahaleCompositeContext(Configuration config) throws Exception {
		super(config);
	}

	public void register() throws Exception {
		MetricRegistry metrics = new MetricRegistry();
		getConfigMap().addProperty(ConfigNames.CODAHALE_REGISTRY, metrics);

		for (Context eachContext : this.getListOfContext()) {
			eachContext.register();
		}
	}

	public void start() {
		for (Context eachContext : this.getListOfContext()) {
			eachContext.start();
		}
	}

	public void stop() {
		for (Context eachContext : this.getListOfContext()) {
			eachContext.stop();
		}

	}

}
