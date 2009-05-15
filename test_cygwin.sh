#!/bin/sh

# $Id: test_cygwin.sh,v 1.1 2002/05/31 06:35:58 boisvert Exp $

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
CLASSPATH="$CLASSPATH;./build/tests"

# Text-based UI:
TESTUI=junit.textui.TestRunner

# AWT-based UI:
# TESTUI=junit.ui.TestRunner

# Swing-based UI:
# TESTUI=junit.swingui.TestRunner

$JAVA -classpath $CLASSPATH $TESTUI "$@"
