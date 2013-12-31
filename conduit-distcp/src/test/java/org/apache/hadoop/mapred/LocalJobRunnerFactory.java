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

package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.JoinPoint;

import java.lang.reflect.Field;
import java.io.IOException;

@Aspect
public class LocalJobRunnerFactory {
  private static final Log LOG = LogFactory.getLog(LocalJobRunnerFactory.class);
  public JobSubmissionProtocol jobRunner = null;

  @Pointcut("execution(* org.apache.hadoop.mapred.JobClient.init(org.apache.hadoop.mapred.JobConf)) && " +
      "args(conf)")
  public void jobClientInit(JobConf conf) {  }

  @Pointcut("execution(* org.apache.hadoop.mapreduce.Job.submit())")
  public void jobSubmit() {  }

  @Before(value = "jobSubmit()")
  public void test() {
    jobRunner = null;
  }

  @After(value = "jobClientInit(conf)")
  public void test(JoinPoint thisPoint, JobConf conf) {
    try {
      if (!conf.get("mapred.job.tracker").equals("local")) return;
      
      JobClient client = (JobClient) thisPoint.getThis();
      Field submissionClient = client.getClass().getDeclaredField("jobSubmitClient");
      submissionClient.setAccessible(true);
      if (jobRunner != null) {
        submissionClient.set(client, jobRunner);
      } else {
        jobRunner = (JobSubmissionProtocol) submissionClient.get(client);
      }
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
    }
  }
}
