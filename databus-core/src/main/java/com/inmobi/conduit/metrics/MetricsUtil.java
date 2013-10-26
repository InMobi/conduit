package com.inmobi.conduit.metrics;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Util class to get stream names from paths/keys
 */
public class MetricsUtil {

	private static Map<String, String> pathToStreamCache = new HashMap<String, String>();

	/**
	 * eg
	 * file:/tmp/databustest1/system/tmp/LocalStreamService_testcluster1_test1@/
	 * jobOut/test1/col1-test1-2013-10-09-10-41_00000.gz should output test1
	 * 
	 * @param path
	 * @return
	 */
	public static String getStreamNameFromTmpPath(String path) {
		try {
			String streamName = pathToStreamCache.get(path);
			if (streamName == null) {
				if (path.contains("/system/tmp")) {
					StringTokenizer st = new StringTokenizer(path, "/");
					List<String> arrayListInst = new LinkedList<String>();
					while (st.hasMoreTokens()) {
						arrayListInst.add(0, st.nextToken());
					}
					String fileName = arrayListInst.get(0);
					st = new StringTokenizer(fileName, "-");
					st.nextToken();
					streamName = st.nextToken();
				} else if (path.contains("/data/")) {
					StringTokenizer st = new StringTokenizer(path, "/");
					List<String> arrayListInst = new LinkedList<String>();
					while (st.hasMoreTokens()) {
						arrayListInst.add(0, st.nextToken());
					}
					streamName = arrayListInst.get(2);

				} else {
					return getStreamNameFromPath(path);
				}

				pathToStreamCache.put(path, streamName);
			}
			return streamName;
		} catch (Exception ex) {
			return null;
		}
	}

	/**
	 * eg /tmp/databustest1/streams_local/test1/2013/10/16/13/52
	 * 
	 * @param path
	 * @return
	 */
	public static String getStreamNameFromPath(String path) {
		try {
			String streamName = pathToStreamCache.get(path);
			if (streamName == null) {
				StringTokenizer st = new StringTokenizer(path, "/");
				List<String> arrayListInst = new LinkedList<String>();
				while (st.hasMoreTokens()) {
					String token = st.nextToken();
					// ignore files
					if (!token.contains(".")) {
						arrayListInst.add(0, token);
					}
				}
				streamName = arrayListInst.get(5);
				pathToStreamCache.put(path, streamName);
			}
			return streamName;
		} catch (Exception ex) {
			return null;
		}
	}

	/**
	 * eg LocalStreamService_test1_col1 should give test1
	 * 
	 * @param path
	 * @return
	 */
	public static String getSteamNameFromCheckPointKey(String key) {
		try {
			String streamName = pathToStreamCache.get(key);
			if (streamName == null) {
				StringTokenizer st = new StringTokenizer(key, "_");
				st.nextToken();
				streamName = st.nextToken();
				pathToStreamCache.put(key, streamName);
			}
			return streamName;
		} catch (Exception ex) {
			return null;
		}
	}

}
