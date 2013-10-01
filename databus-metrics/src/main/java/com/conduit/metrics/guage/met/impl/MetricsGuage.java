package com.conduit.metrics.guage.met.impl;

import com.conduit.metrics.guage.Guage;


public class MetricsGuage<T> implements Guage<T> {

	T value;

	public MetricsGuage(T value) {
		this.value = value;

	}

	public void setValue(T value) {

		this.value = value;
	};
}
