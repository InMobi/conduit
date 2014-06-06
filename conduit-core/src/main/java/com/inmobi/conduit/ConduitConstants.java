/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.inmobi.conduit;

public interface ConduitConstants {
  public static final String CLUSTERS_TO_PROCESS = "com.inmobi.conduit" +
  ".clusters";
  public static final String ZK_ADDR = "com.inmobi.conduit.zkconnect";
  public static final String CONDUIT_XML = "com.inmobi.conduit.cfg";
  public static final String LOG4J_FILE = "com.inmobi.conduit.log4j";
  public static final String KRB_PRINCIPAL = "com.inmobi.conduit.krb" +
  ".principal";
  public static final String KEY_TAB_FILE = "com.inmobi.conduit.keytab";
  public static final String ENABLE_ZOOKEEPER = "com.inmobi.conduit.enablezk";
  public static final String CLUSTER_NAME = "com.inmobi.conduit.current" +
      ".cluster";
  public static final String MB_PER_MAPPER = "com.inmobi.conduit.MBPerMapper";
  public static final String STREAMS_PER_LOCALSERVICE = "com.inmobi.conduit."
      + "streamsPerLocal";
  public static final String AUDIT_PUBLISHER_CONFIG_FILE = "com.inmobi.conduit." +
  		"audit.publisher.config";
  public static final String STREAMS_PER_MERGE = "com.inmobi.conduit."
      + "streamsPerMerge";
  public static final String STREAMS_PER_MIRROR = "com.inmobi.conduit."
      + "streamsPerMirror";
  public static final String DIR_PER_DISTCP_PER_STREAM = "com.inmobi.conduit.distcp."
      + "dirsPerStream";
  public static final String PERGER_ENABLED = "com.inmobi.conduit.purgerEnabled";
  public static final String NUM_RETRIES = "com.inmobi.conduit.retries";
  public static final String AUDIT_ENABLED_KEY = "audit.enabled";
  public static final String AUDIT_COUNTER_GROUP = "audit";
  public static final String AUDIT_COUNTER_NAME_DELIMITER = "#";
  public static final String FILES_PER_COLLECETOR_PER_LOCAL_STREAM = "com.inmobi.conduit."
      + "filesPerCollectorPerLocalStream";
}
