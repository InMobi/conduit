package com.inmobi.conduit.local;

import com.inmobi.conduit.ConfigConstants;
import com.inmobi.conduit.utils.S3FileSystemHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
 * In order to use the class, in conduit.xml for the specific S3N hdfs url set
 * copyMapperClass = com.inmobi.conduit.local.S3NCopyMapper
 *
 * Example
 * <code>
 *   <cluster name="testcluster1" hdfsurl="s3n:///"
              jturl="local"
              jobqueuename="default"
              copyMapperClass = "com.inmobi.conduit.local.S3NCopyMapper"/>
 * </code>
 */

public class S3NCopyMapper extends Mapper<Text, FileStatus, Text,
    Text> implements ConfigConstants {

  private static final Log LOG = LogFactory.getLog(CopyMapper.class);
  private S3FileSystemHelper helper;
  private Configuration srcConf = null;

  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    srcConf = new Configuration();
    srcConf.set(FS_DEFAULT_NAME_KEY,
        context.getConfiguration().get(SRC_FS_DEFAULT_NAME_KEY));
    helper = S3FileSystemHelper.getInstance(context.getConfiguration());
  }


  @Override
  public void map(Text key, FileStatus value, Context context) throws IOException,
    InterruptedException {
    Path src = value.getPath();
    String dest = key.toString();
    String collector = src.getParent().getName();

    if (srcConf == null) {
      srcConf = new Configuration();
      srcConf.set(FS_DEFAULT_NAME_KEY,
          context.getConfiguration().get(SRC_FS_DEFAULT_NAME_KEY));
    }
    FileSystem fs = FileSystem.get(srcConf);
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
