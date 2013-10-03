package com.conduit.metrics.context.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;

import com.conduit.metrics.MetricsType;
import com.conduit.metrics.context.Context;
import com.conduit.metrics.context.ContextType;
import com.conduit.metrics.context.factory.ContextFactory;
import com.conduit.metrics.util.ConfigNames;

/**
 * Should be able to output to multiple outlets eg Console/Ganglia 
 * @author samar.kumar
 *
 */
public abstract class CompositeContext implements Context {

	List<Context> listOfContext = new ArrayList<Context>();
	Configuration config;

	ContextType cType = ContextType.COMPOSITE;

	public CompositeContext(Configuration config) throws Exception {
		this.config = config;

		@SuppressWarnings("unchecked")
		List<ContextType> contextTypeList = (List<ContextType>) this.config.getProperty(ConfigNames.CONTEXTS_TYPE);
		MetricsType metricsType = (MetricsType) this.config.getProperty(ConfigNames.METRICS_IMPLTYPE);
		for (ContextType eachContextType : contextTypeList) {
			switch (eachContextType) {
			case CONSOLE:
				ContextFactory.getConsoleContext(metricsType, config);
			case GANGLIA:
				ContextFactory.getGangliaContext(metricsType, config);
			default:
				throw new RuntimeException("Unknow Context Type found");
			}
		}
	}

	public List<Context> getListOfContext() {
		return listOfContext;
	}

	public ContextType getType() {
		return cType;

	}

	void addContext(Context ctx) {
		listOfContext.add(ctx);
	}

	public Configuration getConfigMap() {
		return config;
	}

}
