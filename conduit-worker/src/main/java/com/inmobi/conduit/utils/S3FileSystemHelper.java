package com.inmobi.conduit.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.hadoop.fs.s3.S3Exception;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import java.io.IOException;
import java.net.URI;

/**
 * Helper class to manage the S3 File operations which the databus lite mapper
 * performs
 */
public class S3FileSystemHelper {

  private S3Bucket bucket;
  private S3Service service;
  private static S3FileSystemHelper INSTANCE;

  private S3FileSystemHelper() {
  }

  /**
   * Utility method to convert Hadoop Path into S3 keys
   *
   * @param p path to a file
   * @return associated S3 key
   * @throws IllegalArgumentException if path provided is not an absolute path
   */
  private static String pathToKey(Path p) {
    if (!p.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute :" + p);
    }
    return p.toUri().getPath().substring(1); //Remove the leading slash
  }

  public void initalize(URI uri, Configuration conf) throws IOException {
    S3Credentials credentials = new S3Credentials();
    credentials.initialize(uri, conf);
    try {
      AWSCredentials awsCredentials =
          new AWSCredentials(credentials.getAccessKey(),
              credentials.getSecretAccessKey());
      this.service = new RestS3Service(awsCredentials);
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
    bucket = new S3Bucket(uri.getHost());
  }

  /**
   * Method to perform serverside copying of S3 Object,
   * only copy within same bucket is supported currently
   *
   * @param src  source path
   * @param dest destination path
   * @throws IOException if copy fails
   */
  public void copyFile(Path src, Path dest) throws IOException {
    String srcKey = pathToKey(src);
    String destKey = pathToKey(dest);
    try {
      service.copyObject(bucket.getName(), srcKey, bucket.getName(),
          new S3Object(destKey), false);
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  public synchronized static S3FileSystemHelper getInstance(Configuration conf)
      throws IOException {
    if (INSTANCE == null) {
      INSTANCE = new S3FileSystemHelper();
      URI uri = FileSystem.get(conf).getUri();
      INSTANCE.initalize(uri, conf);
    }
    return INSTANCE;
  }
}
