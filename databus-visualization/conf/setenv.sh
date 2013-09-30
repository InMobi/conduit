#! /bin/sh

#hadoop-home-path is path till hadoop core jar
export JAVA_OPTS="$JAVA_OPTS -Dhadoop.home=<hadoop-home-path>/*.jar"
