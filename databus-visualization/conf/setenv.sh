#!/bin/sh

#hadoop home path
HADOOP_HOME=/usr/lib/hadoop-all
count=$(ls $HADOOP_HOME/lib/guava-[0-9]*jar | wc -l )
if test $count -gt 1
then
   echo "More than one guava jar available"
   exit
fi
export JAVA_OPTS="$JAVA_OPTS -Dhadoop.home=$HADOOP_HOME -Dguava.home=`ls $HADOOP_HOME/lib/guava-[0-9]*jar`"

