#!/bin/sh

curr_path=`pwd`
if [ -f $curr_path/wave.pid ] ; then
    echo "found wave.pid , please run stop.sh first." 2>&2
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

#set JAVA_OPTS for G1 collector
#set JAVA_OPTS
JAVA_OPTS="-server -Xms12g -Xmx12g -Xss1024k"
#performance Options
JAVA_OPTS="$JAVA_OPTS -XX:MaxTenuringThreshold=15"
JAVA_OPTS="$JAVA_OPTS -XX:+UnlockExperimentalVMOptions"
JAVA_OPTS="$JAVA_OPTS -XX:MaxGCPauseMillis=200"
JAVA_OPTS="$JAVA_OPTS -XX:InitiatingHeapOccupancyPercent=20"
JAVA_OPTS="$JAVA_OPTS -XX:G1MaxNewSizePercent=75"
JAVA_OPTS="$JAVA_OPTS -XX:G1NewSizePercent=40"
JAVA_OPTS="$JAVA_OPTS -XX:MetaspaceSize=512m -XX:MaxMetaspaceSize=512m"
JAVA_OPTS="$JAVA_OPTS -XX:+AggressiveOpts"
JAVA_OPTS="$JAVA_OPTS -XX:+UseBiasedLocking"
JAVA_OPTS="$JAVA_OPTS -XX:+UseFastAccessorMethods"
JAVA_OPTS="$JAVA_OPTS -XX:+DisableExplicitGC"
JAVA_OPTS="$JAVA_OPTS -XX:+UseG1GC"
JAVA_OPTS="$JAVA_OPTS -XX:+UseStringDeduplication"
JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8"

#set HOME
CURR_DIR=`pwd`
cd `dirname "$0"`/..
WAVE_HOME=`pwd`

# echo $WAVE_HOME

if [ -f $WAVE_HOME/bin/wave.pid ] ; then
    echo "found wave.pid , please run stop.sh first." 2>&2
    exit 1
fi

# make log file
if [ ! -d $WAVE_HOME/logs  ] ; then
    mkdir -p $WAVE_HOME/logs
    echo "make dir $WAVE_HOME/logs"
fi

cd $CURR_DIR
if [ -z "$WAVE_HOME" ] ; then
    echo
    echo "Error: WAVE_HOME environment variable is not defined correctly."
    echo
    exit 1
fi
#==============================================================================

#set CLASSPATH
WAVE_CLASSPATH="$WAVE_HOME/conf:$WAVE_HOME/lib/classes"

# check dir exists
if [ ! -d $WAVE_HOME/lib ] ; then
    echo "no lib file exist on $WAVE_HOME"
    exit 1
fi

for i in "$WAVE_HOME"/lib/*.jar
do
    WAVE_CLASSPATH="$WAVE_CLASSPATH:$i"
done

sed -i 's/flush.binlog.offset=true/flush.binlog.offset=false/g' $WAVE_HOME/conf/realtime.properties

#==============================================================================
#startup Server
RUN_CMD="$JAVA_HOME/bin/java"
RUN_CMD="$RUN_CMD -Dwave.home=\"$WAVE_HOME\" -DappName=wave"
RUN_CMD="$RUN_CMD -classpath \"$WAVE_CLASSPATH\""
RUN_CMD="$RUN_CMD $JAVA_OPTS"
RUN_CMD="$RUN_CMD com.jd.binlog.ServerStart $@"
RUN_CMD="$RUN_CMD >> \"$WAVE_HOME/logs/console.log\" 2>&1 &"

cd "$WAVE_HOME"
echo $RUN_CMD
eval $RUN_CMD
echo $! > $WAVE_HOME/bin/wave.pid
#==============================================================================
