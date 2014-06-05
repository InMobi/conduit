#!/usr/bin/env bash

# Licensed under the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Runs a Conduit command as a daemon.

usage="Usage: conduit.sh [start/stop] [<conf-file>]"
CONDUIT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../"
echo "Running Conduit from $CONDUIT_DIR"

#echo $#
# if no args specified, show usage
if [ $# -gt 3 ]; then
  echo $usage
  exit 1
fi

# get arguments
var1=$1
shift
var2=$1
shift
var3=$1
shift

startStop=$var1
configFile=$var2
configDir=$(dirname $configFile)
envfile=$configDir/conduit.env

if [ "$var1" == "start" ] || [ "$var1" == "stop" ] || [ "$var1" == "restart" ]
then
#check config existence
if ! [ -r $configFile ]; then
   echo $configFile " not found."
   echo $usage
   exit 1
fi

#check env existence
if ! [ -r $envfile ]; then
   echo $envfile " not found."
   exit 1
fi

. $envfile

#create PID dir
if [ "$CONDUIT_PID_DIR" = "" ]; then
  CONDUIT_PID_DIR=/tmp/conduit
fi
export _CONDUIT_DAEMON_PIDFILE=$CONDUIT_PID_DIR/conduit.pid

fi

if [ -z $HADOOP_HOME ]; then
  echo "Please define HADOOP_HOME to point to hadoop installation directory."
  exit 1
fi

if [ -z $HADOOP_CONF_DIR ]; then
  echo "Please define HADOOP_CONF_DIR to point to hadoop configuration. eg:: /etc/hadoop/conf"
  exit 1
fi

#set classpath
for f in $HADOOP_HOME/hadoop-*.jar;do
  if [[ "$f" != *tool* ]]; then
    export CLASSPATH=$CLASSPATH:$f
  fi
done
export CLASSPATH=$CLASSPATH:`ls $HADOOP_HOME/lib/*jar | tr "\n" :`;
export CLASSPATH=$CLASSPATH:`ls $CONDUIT_DIR/lib/*jar | tr "\n" :`;
export CLASSPATH=$configDir:$CLASSPATH:$HADOOP_CONF_DIR:$CONDUIT_DIR/bin
export CONDUIT_OPTS="-Dcom.sun.management.jmxremote.port=9089 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
#echo setting classPath to $CLASSPATH

case $startStop in

  (start)

    mkdir -p "$CONDUIT_PID_DIR"

    if [ -f $_CONDUIT_DAEMON_PIDFILE ]; then
      if kill -0 `cat $_CONDUIT_DAEMON_PIDFILE` > /dev/null 2>&1; then
        echo CONDUIT running as process `cat $_CONDUIT_DAEMON_PIDFILE`.  Stop it first.
        exit 1
      fi
    fi

    echo starting CONDUIT

   nohup java $CONDUIT_OPTS  -Dsun.net.client.defaultConnectTimeout=60000 -Dsun.net.client.defaultReadTimeout=60000 -cp "$CLASSPATH" com.inmobi.conduit.Conduit $configFile 2>&1 &
   if [ $? -eq 0 ]
    then
      if /bin/echo -n $! > "$_CONDUIT_DAEMON_PIDFILE"
      then
        sleep 1
        echo CONDUIT STARTED
      else
        echo FAILED TO WRITE PID
        exit 1
      fi
    else
      echo CONDUIT DID NOT START
      exit 1
    fi
    ;;
          
  (stop)

    if [ -f $_CONDUIT_DAEMON_PIDFILE ]; then
      if kill -0 `cat $_CONDUIT_DAEMON_PIDFILE` > /dev/null 2>&1; then
        echo -n Please be patient. It may take upto 1 min or more in stopping CONDUIT..
        kill -s SIGTERM `cat $_CONDUIT_DAEMON_PIDFILE`
      while :
        do 
          if kill -0 `cat $_CONDUIT_DAEMON_PIDFILE` > /dev/null 2>&1; then
             echo -n "."
             sleep 1
          else
             break
          fi
        done
        rm -rf  $_CONDUIT_DAEMON_PIDFILE
        echo DONE
      else
        echo no CONDUIT to stop
      fi
    else
      echo no CONDUIT to stop
    fi
    ;;

  (restart)
    $0 stop $configFile
    $0 start $configFile
    ;;

  (collapse)

     hdfsName=$var2
     dir=$var3
     java -cp "$CLASSPATH" com.inmobi.conduit.utils.CollapseFilesInDir $hdfsName $dir
     ;;

  (*)
    echo $usage
    exit 1
    ;;

esac

