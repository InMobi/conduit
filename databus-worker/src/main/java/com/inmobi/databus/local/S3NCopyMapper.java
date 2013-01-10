package com.inmobi.databus.local;

import com.inmobi.databus.utils.S3FileSystemHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;

/**
 * Top level S3N copy mapper class which uses the S3 Native copy for the copy
 * mapper operations.
 *
 * In order to use the class, in databus.xml for the specific S3N hdfs url set
 * copyMapperClass = com.inmobi.databus.local.S3NCopyMapper
 *
 * Example
 * <code>
 *   <cluster name="testcluster1" hdfsurl="s3n:///"
              jturl="local"
              jobqueuename="default"
              copyMapperClass = "com.inmobi.databus.local.S3NCopyMapper"/>
 * </code>
 */

public class S3NCopyMapper extends Mapper<Text, Text, Text, Text> {

  private static final Log LOG = LogFactory.getLog(CopyMapper.class);
  private S3FileSystemHelper helper;

  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    helper = S3FileSystemHelper.getInstance(context.getConfiguration());
  }


  @Override
  public void map(Text key, Text value, Context context) throws IOException,
    InterruptedException {
    Path src = new Path(key.toString());
    String dest = value.toString();
    String collector = src.getParent().getName();

    FileSystem fs = FileSystem.get(context.getConfiguration());
    // Create Destination directories
    fs.mkdirs(new Path(dest).makeQualified(fs));
    String pathToken;

    pathToken = dest + File.separator + collector + "-"
      + src.getName();
    Path destPath = new Path(pathToken);
    LOG.info("Copying the uncompressed data from src :" + src + " to destination "
      + destPath);
    helper.copyFile(src, destPath);
  }
}
