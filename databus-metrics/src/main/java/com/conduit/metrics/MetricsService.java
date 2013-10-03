package com.conduit.metrics;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.conduit.metrics.context.factory.ContextFactory;
import com.conduit.metrics.context.impl.CompositeContext;
import com.conduit.metrics.guage.Counter;
import com.conduit.metrics.guage.Guage;
import com.conduit.metrics.guage.MeasureFactory;
import com.conduit.metrics.util.ConfigNames;
import com.conduit.metrics.util.InitializationUtil;

public class MetricsService {

	private MetricsType type = null;
	private Configuration config = null;

	private CompositeContext allContext = null;

	private Map<String, Counter> counterMap = null;
	private Map<String, Guage> guageMap = new HashMap<String, Guage>();

	public MetricsService(Configuration config) throws Exception {
		this.config = config;
		counterMap = new HashMap<String, Counter>();
		guageMap = new HashMap<String, Guage>();

		init(config);

	}

	/**
	 * Read the config and create the initParams. It would mainly contais the
	 * metrics implementation eg metrics.codahale and different reported to use
	 * eg Ganglia/console and each config eg gangia host /ganlia port etc
	 * 
	 * @throws Exception
	 */
	private void init(Configuration config) throws Exception {
		InitializationUtil.initilizae(config);

		MetricsType metricType = (MetricsType) config.getProperty(ConfigNames.PRIVATE_METRICSTYPE_IMPL);
		this.type = metricType;
		CompositeContext allContext = ContextFactory.getCompositeContext(metricType, config);
		allContext.register();
		allContext.start();

	}

	public void addCounter(String name) {
		Counter memoryCounter = MeasureFactory.createCounter(getType(), name, this.getConfig());
		if (counterMap.get(name) == null) {
			counterMap.put(name, memoryCounter);
		} else {
			throw new RuntimeException("Counter " + name + " already exisits");
		}
	}

	public void addGuage(String name, Number value) {
		Guage guage = MeasureFactory.createLongGuage(getType(), name, this.getConfig(), value);
		if (guageMap.get(name) == null) {
			guageMap.put(name, guage);
		} else {
			throw new RuntimeException("Guage " + name + " already exisits");
		}
	}

	public void updateGuage(String name, Number value) {
		Guage guage = guageMap.get(name);

		if (guage == null) {
			throw new RuntimeException("Guange does not exist");
		}
		guage.setValue(value);
	}

	public Number getGuageValue(String name) {
		Guage guage = guageMap.get(name);

		if (guage == null) {
			throw new RuntimeException("Guange does not exist");
		}
		return guage.getValue();
	}

	public void incCounter(String name) {
		Counter counter = counterMap.get(name);
		if (counter == null) {
			throw new RuntimeException("Counter does not exist");
		}
		counter.inc();
	}

	public void incCounter(String name, long value) {
		Counter counter = counterMap.get(name);
		if (counter == null) {
			throw new RuntimeException("Counter does not exist");
		}
		counter.inc(value);
	}

	public void decCounter(String name) {
		Counter counter = counterMap.get(name);
		if (counter == null) {
			throw new RuntimeException("Counter does not exist");
		}
		counter.dec();
	}

	public void decCounter(String name, long value) {
		Counter counter = counterMap.get(name);
		if (counter == null) {
			throw new RuntimeException("Counter does not exist");
		}
		counter.dec(value);
	}

	public long getCounterValue(String name) {
		Counter counter = counterMap.get(name);
		if (counter == null) {
			throw new RuntimeException("Counter does not exist");
		}

		return counter.getValue();

	}

	public Configuration getConfig() {
		return config;
	}

	public MetricsType getType() {
		return type;
	}

	void stopAll() {
		this.allContext.stop();
	}

}
