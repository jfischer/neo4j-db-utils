#!/bin/bash
# Copyright 2018 by Jeff Fischer. Licensed under the 3-clause BSD license.
#
# Scrip to create/startu/stop/destroy neo4j instances
# using docker
NEO_VERSION=3.3

function print_usage {
    echo "Usage: neo4j.sh start|stop|status|create|destroy NEO4J_HOME"
    echo "  NEO4J_HOME is the directory where the Neo4j data, log and cid-files will be kept"
    echo "  It can also be specified as an environment variable."
}

if [[ "$#" == "2" ]]; then
  CMD=$1
  ROOT_DIR_RELATIVE=$2
elif [[ "$#" == "1" ]]; then
  if [[ "$NEO4J_HOME" == "" ]]; then
    echo "$0: NEO4J_HOME not specified or set as an environment variable"
    print_usage
    exit 1
  else
    CMD=$1
    ROOT_DIR_RELATIVE=$NEO4J_HOME
    echo "Using $ROOT_DIR_RELATIVE as neo4j root directory"
  fi
else
   echo "$0: Wrong number of arguments"
   print_usage
   exit 1
fi

ROOT_DIR=`cd $ROOT_DIR_RELATIVE; pwd`
DATA_DIR=`cd $ROOT_DIR/data; pwd`
LOGS_DIR=`cd $ROOT_DIR/log; pwd`
CID_DIR=`cd $ROOT_DIR/cid-files; pwd`
#IMPORTS_DIR=`cd $ROOT_DIR/imports; pwd`

function get_password {
  read -p "Please enter the neo4j password:" PASSWORD
}
function get_password_first_time {
  read -p "Please enter a password for neo4j:" PASSWORD
}

function check_existing_environment {
    if [ ! -d $ROOT_DIR ]; then
      echo "$0: Did not find root directory $ROOT_DIR"
      exit 1
    fi
    if [ ! -d $DATA_DIR ]; then
      echo "$0: Did not find data directory $DATA_DIR"
      exit 1
    fi
    if [ ! -d $LOGS_DIR ]; then
      echo "$0: Did not find log directory $LOGS_DIR"
      exit 1
    fi
    if [ ! -d $CID_DIR ]; then
      echo "$0: Did not find container id file directory $CID_DIR"
      exit 1
    fi
    #if [ ! -d $IMPORTS_DIR ]; then
    #  echo "$0: Did not find import file directory $IMPORTS_DIR"
    #  exit 1
    #fi
}

function make_dirs_if_needed {
  mkdir -p $ROOT_DIR
  mkdir -p $DATA_DIR
  mkdir -p $LOGS_DIR
  mkdir -p $CID_DIR
  #mkdir -p $IMPORTS_DIR
}

CID_FILE=$CID_DIR/neo4j.cid
if [ -f $CID_FILE ]; then
  CID=`cat $CID_FILE`
else
  CID=""
fi

# This is really about the commercial vs community edition
if [[ "`uname`" == "Linux" ]]; then
  USER_MAP_ARGS="-u `id -u`:`id -g` --userns=host -v /etc/group:/etc/group:ro -v /etc/passwd:/etc/passwd:ro"
else
  USER_MAP_ARGS=""
fi

if [[ "$1" == "start" ]]; then
  if [[ "$CID" == "" ]]; then
    echo "Running new container"
    make_dirs_if_needed
    get_password
    echo docker run -d $USER_MAP_ARGS \
           --publish=7474:7474 --publish=7687:7687 \
           --cidfile=$CID_FILE \
           --volume=$DATA_DIR:/data \
           --volume=$LOGS_DIR:/logs \
           --volume=`pwd`:/var/lib/neo4j/import \
           --name=neo4j-container \
           --env=NEO4J_AUTH=neo4j/$PASSWORD \
           --env=NEO4J_dbms_memory_pagecache_size=1024M \
           --env=NEO4J_dbms_memory_heap_maxSize=1024M \
           neo4j:$NEO_VERSION
    docker run -d $USER_MAP_ARGS \
    --publish=7474:7474 --publish=7687:7687 \
    --cidfile=$CID_FILE \
    --volume=$ROOT_DIR:/data \
    --volume=`pwd`:/var/lib/neo4j/import \
    --volume=$LOGS_DIR:/logs \
    --name=neo4j-container \
    --env=NEO4J_AUTH=neo4j/$PASSWORD \
     --env=NEO4J_dbms_memory_pagecache_size=1024M \
     --env=NEO4J_dbms_memory_heap_maxSize=1024M \
    neo4j:$NEO_VERSION
  else
    RUNNING=`docker inspect -f {{.State.Running}} $CID`
    if [[ "$RUNNING" == "true" ]]; then
      echo "Container $CID is already running"
      exit 0
    else
      echo "Starting container $CID"
      docker start $CID
    fi
  fi
elif [[ "$1" == "stop" ]]; then
  if [[ "$CID" == "" ]]; then
    echo "No container id file found at $CID_FILE"
    exit 1
  else
    echo "Stopping container $CID"
    docker stop $CID
  fi
elif [[ "$1" == "status" ]]; then
  if [[ "$CID" == "" ]]; then
    echo "No container id file for neo4j found at $CID_FILE"
    exit 1
  else
    RUNNING=`docker inspect -f {{.State.Running}} $CID`
    if [[ "$RUNNING" == "true" ]]; then
      echo "Container $CID is running"
      exit 0
    elif [[ "$RUNNING" == "false" ]]; then
      echo "Container $CID is not running"
      exit 2
    else
      echo "Problem with docker inspect of $CID"
      exit 1
    fi
  fi
elif [[ "$1" == "destroy" ]]; then
  read -p "Are you sure you want to destroy your neo4j instance (y/n)?" choice
  case "$choice" in 
    y|Y ) echo "ok, continuing";;
    n|N ) echo "exiting!"; exit 1;;
    * ) echo "invalid input!"; exit 1;;
  esac
  if [[ "$CID" == "" ]]; then
    echo "No container id file found at $CID_FILE"
  else
    echo "Removing container $CID"
    echo docker stop $CID
    docker stop $CID
    echo docker rm $CID
    docker rm $CID
    rm $CID_FILE
  fi
  echo sudo rm -rf $ROOT_DIR/databases $ROOT_DIR/dbms
  sudo rm -rf $ROOT_DIR/databases $ROOT_DIR/dbms
elif [[ "$1" == "create" ]]; then
  make_dirs_if_needed
  # check for error situations
  #if [ ! -d $IMPORTS_DIR ]; then
  #  echo "$0: Did not find import file directory $IMPORTS_DIR"
  #  exit 1
  #fi
  if [[ "$CID" != "" ]]; then
    RUNNING=`docker inspect -f {{.State.Running}} $CID`
    if [[ "$RUNNING" == "true" ]]; then
      echo "Container $CID is already running!"
      exit 1
    else
      echo "There is a CID file at $CID_FILE"
      echo "Perhaps this is left from a previous install....aborting."
      exit 1
    fi      
  fi

  # find the files to import
  NODES_ARGS=""
  EDGES_ARGS=""
  #cd $IMPORTS_DIR
  for f in nodes-*.csv
  do
      echo "Processing $f"
      NODES_ARGS="$NODES_ARGS --nodes /var/lib/neo4j/import/$f"
 done
  for f in edges-*_to_*.csv
  do
      echo "Processing $f"
      EDGES_ARGS="$EDGES_ARGS --relationships /var/lib/neo4j/import/$f"
  done
  #cd $SAVEDIR

  # remove the old database files
  echo rm -rf $DATA_DIR/databases $DATA_DIR/dbms
  rm -rf $DATA_DIR/databases $DATA_DIR/dbms

  get_password_first_time

  # do the load
  echo "Starting load in container. Load command is: bin/neo4j-admin import $NODES_ARGS $EDGES_ARGS"
  docker run -it --rm \
   $USER_MAP_ARGS \
   --volume=$DATA_DIR:/data \
   --volume=`pwd`:/var/lib/neo4j/import \
   --volume=$LOGS_DIR:/logs \
   --env=NEO4J_AUTH=neo4j/$PASSWORD \
     --env=NEO4J_dbms_memory_pagecache_size=1024M \
     --env=NEO4J_dbms_memory_heap_maxSize=1024M \
   neo4j-loader bin/neo4j-admin import $NODES_ARGS $EDGES_ARGS
  rc=$?
  if [[ "$rc" != "0" ]]; then
      echo "Errors in import"
      exit 1
  fi

  # start the real container using the created db files
  echo "Starting neo4j server..."
  docker run -d \
   $USER_MAP_ARGS \
   --publish=7474:7474 --publish=7687:7687 \
   --cidfile=$CID_FILE \
   --volume=$DATA_DIR:/data \
   --volume=`pwd`:/var/lib/neo4j/import \
   --volume=$LOGS_DIR:/logs \
   --name=neo4j-container \
   --env=NEO4J_AUTH=neo4j/$PASSWORD \
     --env=NEO4J_dbms_memory_pagecache_size=1024M \
     --env=NEO4J_dbms_memory_heap_maxSize=1024M \
   neo4j:$NEO_VERSION
  rc=$?
  if [[ "$rc" != "0" ]]; then
      echo "Problem with start"
      exit 1
  fi
  echo "Database import and creation successful!"
  
else
  echo "$0: Unknown command $1"
  print_usage
  exit 1
fi
exit 0
