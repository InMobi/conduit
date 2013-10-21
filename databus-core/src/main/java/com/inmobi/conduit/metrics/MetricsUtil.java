package com.inmobi.conduit.metrics;

import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
/**
 * Util class to get stream names from paths/keys
 */
public class MetricsUtil {

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
			StringTokenizer st = new StringTokenizer(path, "/");
			List<String> arrayListInst = new LinkedList<String>();
			while(st.hasMoreTokens()){
				arrayListInst.add(0, st.nextToken());
			}
			return arrayListInst.get(1);
		} catch (Exception ex) {
			return null;
		}
	}
	
	
	
	/**
	 * eg
	 * /tmp/databustest1/streams_local/test1/2013/10/16/13/52
	 * 
	 * @param path
	 * @return
	 */
	public static String getStreamNameFromPath(String path) {
		try {
			StringTokenizer st = new StringTokenizer(path, "/");
			List<String> arrayListInst = new LinkedList<String>();
			while (st.hasMoreTokens()) {
				String token = st.nextToken();
				//ignore files
				if (!token.contains(".")) {
					arrayListInst.add(0, token);
				}
			}
			return arrayListInst.get(5);
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
			StringTokenizer st = new StringTokenizer(key, "_");
			st.nextToken();
			return st.nextToken();
		} catch (Exception ex) {
			return null;
		}
	}
	
	
	public static String getStreamNameFromExistsPath(String path) {
		try {
			StringTokenizer st = new StringTokenizer(path, "/");
			List<String> arrayListInst = new LinkedList<String>();
			while(st.hasMoreTokens()){
				arrayListInst.add(0, st.nextToken());
			}
			return arrayListInst.get(6);
		} catch (Exception ex) {
			return null;
		}
	}
	
	/**
	*Get the stream name from the purge path
	*/
	public static String getStreamNameFromPurgeDir(String path) {
		try {
			StringTokenizer st = new StringTokenizer(path, "/");
			List<String> arrayListInst = new LinkedList<String>();
			while (st.hasMoreTokens()) {
				arrayListInst.add(0, st.nextToken());
			}
			return arrayListInst.get(4);
		} catch (Exception ex) {
			return null;
		}
	}
}
