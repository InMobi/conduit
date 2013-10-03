package com.conduit.metrics.guage;

public interface Counter {
	void inc();

	void dec();

	void inc(long inc);

	void dec(long dec);

	long getValue();
}
