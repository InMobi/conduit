package com.inmobi.databus.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

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

public class FileUtil {
  private static final Log LOG = LogFactory.getLog(FileUtil.class);

  public static void gzip(Path src, Path target, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream out = fs.create(target);
    GzipCodec gzipCodec = (GzipCodec) ReflectionUtils.newInstance(
        GzipCodec.class, conf);
    Compressor gzipCompressor = CodecPool.getCompressor(gzipCodec);
    OutputStream compressedOut = gzipCodec.createOutputStream(out,
        gzipCompressor);
    FSDataInputStream in = fs.open(src);
    try {
      IOUtils.copyBytes(in, compressedOut, conf);
    } catch (Exception e) {
      LOG.error("Error in compressing ", e);
    } finally {
      in.close();
      CodecPool.returnCompressor(gzipCompressor);
      compressedOut.close();
      out.close();
    }
  }
  
  // This method returns the name of the jar containing the input class.
  // It is taken from org.apache.hadoop.mapred.JobConf class.
  public static String findContainingJar(Class my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for(Enumeration itr = loader.getResources(class_file);
          itr.hasMoreElements();) {
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
}
