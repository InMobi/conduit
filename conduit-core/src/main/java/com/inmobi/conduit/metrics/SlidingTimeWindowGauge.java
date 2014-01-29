package com.inmobi.conduit.metrics;

import java.util.concurrent.TimeUnit;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.SlidingTimeWindowReservoir;

/**
 * A Gauge which is wrapped around a SlidingTimeWindowGuage. This will have a
 * aggregated value for all the values collected for last n seconds.
 * 
 */
public class SlidingTimeWindowGauge implements Gauge<Long> {

  private SlidingTimeWindowReservoir stwR;

  public SlidingTimeWindowGauge(long window, TimeUnit timeUnits) {
    stwR = new SlidingTimeWindowReservoir(window, timeUnits);
  }

  public void setValue(Long value) {
    this.stwR.update(value);
  };

  public Long getValue() {
    long sumOfValues = 0;
    for (long eachValue : stwR.getSnapshot().getValues()) {
      sumOfValues += eachValue;
    }
    return sumOfValues;
  }

}
