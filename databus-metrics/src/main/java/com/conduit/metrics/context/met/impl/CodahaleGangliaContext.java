package com.conduit.metrics.context.met.impl;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.conduit.metrics.context.impl.GangliaContext;
import com.conduit.metrics.util.ConfigNames;

public class CodahaleGangliaContext extends GangliaContext {

	private GangliaReporter reporter;

	private String hostName;
	private int port;

	public CodahaleGangliaContext( Configuration config) {
		super(config);
		this.hostName =  config.getString(ConfigNames.GANGLIA_HOSTNAME);
		this.port = config.getInt(ConfigNames.GANGLIA_PORT);

	}

	public void register() throws Exception {
		MetricRegistry registry = (MetricRegistry) this.getConfigMap().getProperty(ConfigNames.CODAHALE_REGISTRY);
		final GMetric ganglia = new GMetric(this.hostName, this.port, UDPAddressingMode.MULTICAST, 1);
		reporter = GangliaReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build(ganglia);

	}

	public void start() {
		reporter.start(1, TimeUnit.MINUTES);

	}

	public void stop() {
		reporter.stop();
	}

	@Override
	public String getHostName() {
		// TODO Auto-generated method stub
		return this.hostName;
	}

	@Override
	public int getPort() {
		// TODO Auto-generated method stub
		return this.port;
	}

}
