#!/bin/sh

curr_path=`pwd`
if [ -f $curr_path/tower.pid ] ; then
    echo "found tower.pid , please run stop.sh first." 2>&2
    exit 1
fi

#============================================
# get java home
# echo $JAVA_HOME
cd $JAVA_HOME
JAVA=`pwd`
if [ ! -e "$JAVA/bin/java" ] ; then
    echo "no java bin exist"
    exit 1
fi

cd $curr_path
#==============================================================================


#set JAVA_OPTS
JAVA_OPTS="-server -Xms3g -Xmx3g -Xmn1g -Xss2048k"
#performance Options
JAVA_OPTS="$JAVA_OPTS -XX:+AggressiveOpts"
JAVA_OPTS="$JAVA_OPTS -XX:+UseBiasedLocking"
JAVA_OPTS="$JAVA_OPTS -XX:+UseFastAccessorMethods"
JAVA_OPTS="$JAVA_OPTS -XX:+DisableExplicitGC"
JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC"
JAVA_OPTS="$JAVA_OPTS -XX:+UseConcMarkSweepGC"
JAVA_OPTS="$JAVA_OPTS -XX:+CMSParallelRemarkEnabled"
JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSCompactAtFullCollection"
JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSInitiatingOccupancyOnly"
JAVA_OPTS="$JAVA_OPTS -XX:CMSInitiatingOccupancyFraction=75"
JAVA_OPTS="$JAVA_OPTS -XX:MaxPermSize=256m -XX:PermSize=256m"
#GC Log Options
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCApplicationStoppedTime"
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCTimeStamps"
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails"
#debug Options
#JAVA_OPTS="$JAVA_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,address=8065,server=y,suspend=n"
#==============================================================================

#set HOME
CURR_DIR=`pwd`
cd `dirname "$0"`/..
TOWER_HOME=`pwd`

# echo $TOWER_HOME

if [ -f $TOWER_HOME/bin/tower.pid ] ; then
    echo "found tower.pid , please run stop.sh first." 2>&2
    exit 1
fi

# make log file
if [ ! -d $TOWER_HOME/logs  ] ; then
    mkdir -p $TOWER_HOME/logs
    echo "make dir $TOWER_HOME/logs"
fi

cd $CURR_DIR
if [ -z "$TOWER_HOME" ] ; then
    echo
    echo "Error: TOWER_HOME environment variable is not defined correctly."
    echo
    exit 1
fi
#==============================================================================

#set CLASSPATH
TOWER_CLASSPATH="$TOWER_HOME/conf:$TOWER_HOME/lib/classes"

# check dir exists
if [ ! -d $TOWER_HOME/lib ] ; then
    echo "no lib file exist on $TOWER_HOME"
    exit 1
fi

for i in "$TOWER_HOME"/lib/*.jar
do
    TOWER_CLASSPATH="$TOWER_CLASSPATH:$i"
done
#==============================================================================
#startup Server
RUN_CMD="$JAVA_HOME/bin/java"
RUN_CMD="$RUN_CMD -Dtower.home=\"$TOWER_HOME\" -DappName=tower"
RUN_CMD="$RUN_CMD -classpath \"$TOWER_CLASSPATH\""
RUN_CMD="$RUN_CMD $JAVA_OPTS"
RUN_CMD="$RUN_CMD com.jd.binlake.tower.server.TowerStartup $@"
RUN_CMD="$RUN_CMD >> \"$TOWER_HOME/logs/console.log\" 2>&1 &"

cd "$TOWER_HOME"
echo $RUN_CMD
eval $RUN_CMD
echo $! > $TOWER_HOME/bin/tower.pid
#==============================================================================
