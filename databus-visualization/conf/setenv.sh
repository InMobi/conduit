#!/bin/sh

#hadoop home path
HADOOP_HOME=<path to hadoop jars>
echo "Using HADOOP_HOME :	$HADOOP_HOME"
export JAVA_OPTS="$JAVA_OPTS -Dhadoop.home=$HADOOP_HOME -Dguava.home=`ls $HADOOP_HOME/lib/guava-[0-9]*jar`"

