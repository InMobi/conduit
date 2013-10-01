package com.conduit.metrics.context.met.impl;

import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.conduit.metrics.context.impl.ConsoleContext;

public class MetricsConsoleContext extends ConsoleContext {

	public MetricsConsoleContext( Configuration config) {
		super(config);
	}

	public void register() throws Exception {
		MetricRegistry registry = (MetricRegistry) this.getConfigMap().getProperty("registry");
		final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
		reporter.start(1, TimeUnit.MINUTES);

	}

	public void start() {
		// TODO Auto-generated method stub

	}

	public void stop() {
		// TODO Auto-generated method stub

	}
}
