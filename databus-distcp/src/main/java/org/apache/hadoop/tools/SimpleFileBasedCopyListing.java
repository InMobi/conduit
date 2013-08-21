package org.apache.hadoop.tools;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;

/**
 *  This class does not do any check
 **/
public class SimpleFileBasedCopyListing extends CopyListing {

  private final CopyListing simpleListing;

  public SimpleFileBasedCopyListing(Configuration configuration,
      Credentials credentials) {
    super(configuration, credentials);
    simpleListing = new SimpleCopyListing(getConf(), credentials) ;
  }

  /** {@inheritDoc} */
  @Override
  protected void validatePaths(DistCpOptions options)
      throws IOException, InvalidInputException {
  }

  /**
   * Implementation of CopyListing::buildListing().
   * Iterates over all source paths mentioned in the input-file.
   *
   * @param pathToListFile: Path on HDFS where the listing file is written.
   * @param options: Input Options for DistCp (indicating source/target paths.)
   *
   * @throws java.io.IOException
   */
  @Override
  public void doBuildListing(Path pathToListFile, DistCpOptions options)
      throws IOException {
    List<Path> sourcePaths = FileBasedCopyListing.fetchFileList(
        options.getSourceFileListing(), getConf());

    DistCpOptions newOption = new DistCpOptions(sourcePaths,
        options.getTargetPath());
    newOption.setSyncFolder(options.shouldSyncFolder());
    newOption.setOverwrite(options.shouldOverwrite());
    newOption.setDeleteMissing(options.shouldDeleteMissing());
    newOption.setPreserveSrcPath(options.shouldPreserveSrcPath());
    newOption.setSkipPathValidation(options.isSkipPathValidation());
    newOption.setUseSimpleFileListing(options.isUseSimpleFileListing());
    simpleListing.buildListing(pathToListFile, newOption);
  }

  /** {@inheritDoc} */
  @Override
  protected long getBytesToCopy() {
    return simpleListing.getBytesToCopy();
  }

  /** {@inheritDoc} */
  @Override
  protected long getNumberOfPaths() {
    return simpleListing.getNumberOfPaths();
  }

}
