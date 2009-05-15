#!/bin/sh

# $Id: run_cygwin.sh,v 1.2 2003/03/21 03:17:47 boisvert Exp $

if [ -z "$JAVA_HOME" ] ; then
  JAVA=`which java`
  if [ -z "$JAVA" ] ; then
    echo "Cannot find JAVA. Please set your PATH."
    exit 1
  fi
  JAVA_BIN=`dirname $JAVA`
  JAVA_HOME=$JAVA_BIN/..
fi

JAVA=$JAVA_HOME/bin/java

CLASSPATH=`echo lib/*.jar | tr ' ' ';'`";$CLASSPATH"
CLASSPATH="$JAVA_HOME/lib/tools.jar;$CLASSPATH"
CLASSPATH="$CLASSPATH;./build/classes"
CLASSPATH="$CLASSPATH;./build/examples"
CLASSPATH="$CLASSPATH;./build/tests"

$JAVA -classpath $CLASSPATH "$@"
