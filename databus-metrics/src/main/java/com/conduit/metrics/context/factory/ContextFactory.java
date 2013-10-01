package com.conduit.metrics.context.factory;


import org.apache.commons.configuration.Configuration;

import com.conduit.metrics.MetricsType;
import com.conduit.metrics.context.impl.CompositeContext;
import com.conduit.metrics.context.impl.ConsoleContext;
import com.conduit.metrics.context.impl.GangliaContext;
import com.conduit.metrics.context.met.impl.MetricsCompositeContext;
import com.conduit.metrics.context.met.impl.MetricsConsoleContext;
import com.conduit.metrics.context.met.impl.MetricsGangliaContext;

public class ContextFactory {

	public static CompositeContext getCompositeContext(MetricsType type,  Configuration config) throws Exception {
		switch (type) {
		case Metrics:
			CompositeContext compositeContext = new MetricsCompositeContext(config);
			return compositeContext;
		default:
			throw new RuntimeException("default not supported Exception");
		}
	}

	public static ConsoleContext getConsoleContext(MetricsType type,  Configuration config) throws Exception {
		switch (type) {
		case Metrics:
			ConsoleContext compositeContext = new MetricsConsoleContext(config);
			return compositeContext;
		default:
			throw new RuntimeException("default not supported Exception");
		}
	}

	public static GangliaContext getGangliaContext(MetricsType type,  Configuration config) throws Exception {
		switch (type) {
		case Metrics:
			GangliaContext compositeContext = new MetricsGangliaContext(config);
			return compositeContext;
		default:
			throw new RuntimeException("default not supported Exception");
		}
	}

}
