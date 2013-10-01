package com.conduit.metrics.context.met.impl;


import org.apache.commons.configuration.Configuration;

import com.codahale.metrics.MetricRegistry;
import com.conduit.metrics.context.Context;
import com.conduit.metrics.context.impl.CompositeContext;

public class MetricsCompositeContext extends CompositeContext {

	public MetricsCompositeContext( Configuration config) throws Exception {
		super(config);
	}

	public void register() throws Exception {
		MetricRegistry metrics = new MetricRegistry();
		getConfigMap().addProperty("registry", metrics);
		
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
