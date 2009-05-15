#!/bin/sh

# $Id: test.sh,v 1.2 2005/08/22 00:31:16 boisvert Exp $

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

CLASSPATH=`echo lib/*.jar | tr ' ' ':'`:$CLASSPATH
CLASSPATH=$JAVA_HOME/lib/tools.jar:$CLASSPATH
CLASSPATH=$CLASSPATH:./build/classes
CLASSPATH=$CLASSPATH:./build/tests
CLASSPATH=$CLASSPATH:./build/test-classes

# Text-based UI:
# TESTUI=junit.textui.TestRunner

# AWT-based UI:
# TESTUI=junit.ui.TestRunner

# Swing-based UI:
TESTUI=junit.swingui.TestRunner

$JAVA -classpath $CLASSPATH $TESTUI "$@"
