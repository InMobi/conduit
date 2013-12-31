/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.MockJobTracker;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;

public class CounterProvider extends MockJobTracker {
  private static final Log LOG = LogFactory.getLog(CounterProvider.class);

  private Counters counters;

  public CounterProvider(Counters counters) throws IOException {
    this.counters = counters;
  }

  public void setCounters(Counters counters) {
    this.counters = counters;
  }

  public org.apache.hadoop.mapred.Counters getJobCounters(JobID ignore) throws IOException {
    org.apache.hadoop.mapred.Counters retCounter = new org.apache.hadoop.mapred.Counters();
    for (CounterGroup group : counters) {
      for (Counter counter : group) {
        retCounter.incrCounter(group.getName(), counter.getName(), counter.getValue());
      }
    }
    return retCounter;
  }
}
