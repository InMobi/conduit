package com.conduit.metrics.guage.met.impl;

import com.conduit.metrics.guage.Counter;

public class MetricsCounter implements Counter {

	com.codahale.metrics.Counter counter;

	public MetricsCounter(com.codahale.metrics.Counter counter) {
		this.counter = counter;
	}

	public void inc() {
		this.counter.inc();
	}

	public void dec() {
		this.counter.dec();

	}

	public void inc(long inc) {
		this.counter.inc(inc);

	}

	public void dec(long dec) {
		this.counter.dec(dec);

	}

	@Override
	public long getValue() {
		return this.counter.getCount();

	}

}
