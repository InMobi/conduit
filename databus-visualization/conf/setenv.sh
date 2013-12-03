#! /bin/sh

#hadoop home path
HADOOP_HOME=/usr/lib/hadoop-all
for f in $HADOOP_HOME/lib/*.jar;
do
  if echo "$f" | grep 'guava';
  then
    jar=$f
  fi
done
echo "Using HADOOP_HOME: $HADOOP_HOME"
GUAVA_HOME=$jar
echo "Using GUAVA_HOME: $GUAVA_HOME"
export JAVA_OPTS="$JAVA_OPTS -Dhadoop.home=$HADOOP_HOME -Dguava.home=$GUAVA_HOME"

