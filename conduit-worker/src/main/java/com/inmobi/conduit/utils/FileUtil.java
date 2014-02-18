package com.inmobi.conduit.utils;


import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

import com.inmobi.messaging.util.AuditUtil;

public class FileUtil {
  private static final Log LOG = LogFactory.getLog(FileUtil.class);
  private static final int WINDOW_SIZE = 60;

  public static void gzip(Path src, Path target, Configuration conf,
      Map<Long, Long> received) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream out = fs.create(target);
    GzipCodec gzipCodec = ReflectionUtils.newInstance(
        GzipCodec.class, conf);
    Compressor gzipCompressor = CodecPool.getCompressor(gzipCodec);
    OutputStream compressedOut = gzipCodec.createOutputStream(out,
        gzipCompressor);
    FSDataInputStream in = fs.open(src);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        byte[] msg = line.getBytes();
        if (received != null) {
          byte[] decodedMsg = Base64.decodeBase64(msg);
          incrementReceived(decodedMsg, received);
        }
        compressedOut.write(msg);
        compressedOut.write("\n".getBytes());
      }
    } catch (Exception e) {
      LOG.error("Error in compressing ", e);
    } finally {
      IOUtils.cleanup(LOG, in);
      try {
        if (compressedOut != null) {
         compressedOut.close();
        }
        if (out != null) {
          out.close();
        }
      } catch (IOException exception) {
        LOG.error("Could not close output-stream. ", exception);
        throw exception;
      }
      CodecPool.returnCompressor(gzipCompressor);
    }
  }

  private static Long getWindow(Long timestamp) {
    Long window = timestamp - (timestamp % (WINDOW_SIZE * 1000));
    return window;
  }

  private static void incrementReceived(byte[] msg, Map<Long, Long> received) {
    long timestamp = AuditUtil.getTimestamp(msg);
    long window = getWindow(timestamp);
    if (timestamp != -1) {
      if (received.containsKey(window)) {
        received.put(window, received.get(window) + 1);
      } else {
        received.put(window, Long.valueOf(1));
      }
    }
  }

  // This method returns the name of the jar containing the input class.
  // It is taken from org.apache.hadoop.mapred.JobConf class.
  public static String findContainingJar(Class my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration itr = loader.getResources(class_file); itr
          .hasMoreElements();) {
        URL url = (URL) itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          // URLDecoder is a misnamed class, since it actually decodes
          // x-www-form-urlencoded MIME type rather than actual
          // URL encoding (which the file path has). Therefore it would
          // decode +s to ' 's which is incorrect (spaces are actually
          // either unencoded or encoded as "%20"). Replace +s first, so
          // that they are kept sacred during the decoding process.
          toReturn = toReturn.replaceAll("\\+", "%2B");
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
  
  // This method is taken from DistCp SimpleCopyListing class.
  public static FileStatus getFileStatus(FileStatus fileStatus,
      ByteArrayOutputStream buffer, DataInputBuffer in) throws IOException {
    // if the file is not an instance of RawLocaleFileStatus, simply return it
    if (fileStatus.getClass() == FileStatus.class) {
      return fileStatus;
    }
    
    // Else if it is a local file, we need to convert it to an instance of 
    // FileStatus class. The reason is that SequenceFile.Writer/Reader does 
    // an exact match for FileStatus class.
    FileStatus status = new FileStatus();
    
    buffer.reset();
    DataOutputStream out = new DataOutputStream(buffer);
    fileStatus.write(out);
    
    in.reset(buffer.toByteArray(), 0, buffer.size());
    status.readFields(in);
    return status;
  }
  
  /*
   * FileSystem.listStatus behaves differently for different filesystems.
   * Different behaviors by different filesystem are:
   * 
   *                      HDFS          S3N       Local Dir
   * With Data            Array         Array     Array 
   * Empty Directory      Empty Array   null      Empty Array 
   * Non Existent Path    null          null      Empty Array
   * 
   * This helper method would ensure that the output is consistent with HDFS behavior, i.e.
   * 1) If the path is a dir and has data return list of filestatus, 2) If the path is a dir
   * but is empty return empty list, 3) If the path doesn't exist return null.
   */
  public static FileStatus[] listStatusAsPerHDFS(FileSystem fs, Path p)
      throws IOException {
    FileStatus[] fStatus;
    try {
      fStatus = fs.listStatus(p);
    } catch (FileNotFoundException ignore) {
      return null;
    }
    if (fStatus != null && fStatus.length > 0)
      return fStatus;
    if (fs.exists(p))
      return new FileStatus[0];
    return null;

  }

  public static String toStringOfFileStatus(List<FileStatus> list) {
    StringBuffer str = new StringBuffer();
    for (FileStatus f : list) {
      str.append(f.getPath().toString()).append(",");
    }
    return str.toString();
  }
}
