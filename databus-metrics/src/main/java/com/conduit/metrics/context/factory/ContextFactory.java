package com.conduit.metrics.context.factory;


import org.apache.commons.configuration.Configuration;

import com.conduit.metrics.MetricsType;
import com.conduit.metrics.context.impl.CompositeContext;
import com.conduit.metrics.context.impl.ConsoleContext;
import com.conduit.metrics.context.impl.GangliaContext;
import com.conduit.metrics.context.met.impl.CodahaleCompositeContext;
import com.conduit.metrics.context.met.impl.CodahaleConsoleContext;
import com.conduit.metrics.context.met.impl.CodahaleGangliaContext;

public class ContextFactory {

	public static CompositeContext getCompositeContext(MetricsType type,  Configuration config) throws Exception {
		switch (type) {
		case CODAHALE:
			CompositeContext compositeContext = new CodahaleCompositeContext(config);
			return compositeContext;
		default:
			throw new RuntimeException("default/illegal implementation not supported Exception");
		}
	}

	public static ConsoleContext getConsoleContext(MetricsType type,  Configuration config) throws Exception {
		switch (type) {
		case CODAHALE:
			ConsoleContext compositeContext = new CodahaleConsoleContext(config);
			return compositeContext;
		default:
			throw new RuntimeException("default/illegal implementation not supported Exception");
		}
	}

	public static GangliaContext getGangliaContext(MetricsType type,  Configuration config) throws Exception {
		switch (type) {
		case CODAHALE:
			GangliaContext compositeContext = new CodahaleGangliaContext(config);
			return compositeContext;
		default:
			throw new RuntimeException("default/illegal implementation not supported Exception");
		}
	}

}
