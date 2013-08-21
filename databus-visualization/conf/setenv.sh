#! /bin/sh

#e.g. hadoop.home=/usr/lib/hadoop/*.jar
export JAVA_OPTS="$JAVA_OPTS -Dhadoop.home=<hadoop-home-path>/*.jar"
