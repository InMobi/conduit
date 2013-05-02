package com.inmobi.databus.validator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;

public class DatabusValidator {
  private static final Log LOG = LogFactory.getLog(DatabusValidator.class);
  private static int minArgs = 3;

  public DatabusValidator() {
  }

  private static void printUsage() {
    System.out.println("Usage: ");
    System.out.println("-verify " +
        "[-stream (comma separated stream names)]" +
        "[-mode (comma separated stream modes: {local,merge,mirror})]" +
        "[-cluster (comma separated cluster names)]" +
        "<-conf (databus.xml file path)>");
    System.out.println("-fix " +
        "<-stream (stream name)>" +
        "<-mode (stream mode: {local,merge,mirror})>" +
        "<-cluster (cluster name)>" +
        "<-conf (databus.xml file path)>");
  }

  public static void main(String[] args) throws Exception {
    if (args.length < minArgs) {
      printUsage();
      System.exit(-1);
    }
    boolean verify = false;
    boolean fix = false;
    String streams = null;
    String modes = null;
    String clusters = null;
    String databusXmlFile = null;

    if (args[0].equalsIgnoreCase("-verify")) {
      verify = true;
    } else if (args[0].equalsIgnoreCase("-fix")) {
      fix = true;
    } else {
      printUsage();
      System.exit(-1);
    }

    // check each consecutive pair of command options
    for (int i = 1; i < args.length - 1;) {
      if (args[i].equalsIgnoreCase("-stream")) {
        streams = args[i+1];
        i += 2;
      } else if (args[i].equalsIgnoreCase("-mode")) {
        modes = args[i+1];
        i += 2;
      } else if (args[i].equalsIgnoreCase("-cluster")) {
        clusters = args[i+1];
        i += 2;
      } else if (args[i].equalsIgnoreCase("-conf")) {
        databusXmlFile = args[i+1];
        i += 2;
      } else {
        printUsage();
        System.exit(-1);
      }
    }

    // validate the mandatory options
    if (databusXmlFile == null || (fix && (streams == null || modes == null
        || clusters == null))) {
      printUsage();
      System.exit(-1);
    }

    // parse databus.xml
    DatabusConfigParser configParser =
        new DatabusConfigParser(databusXmlFile);
    DatabusConfig config = configParser.getConfig();

    StreamsValidator streamsValidator = new StreamsValidator(config,
        streams, modes, clusters);
    // perform streams verification
    streamsValidator.validateStreams(fix);
  }
}

