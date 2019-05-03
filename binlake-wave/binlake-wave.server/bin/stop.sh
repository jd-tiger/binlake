#!/bin/sh
cd ../
base=`pwd`
# echo $base
pid_file=$base/bin/wave.pid
if [ ! -f "$pid_file" ] ; then
    echo "wave server is not running. exists"
    exit 1
fi

sed -i 's/flush.binlog.offset=false/flush.binlog.offset=true/g' $base/conf/realtime.properties

# 等待2s 等待参数生效
sleep 2s

pid=$(cat $pid_file)

echo -e "stopping wave $pid ... "
kill -9 $pid

sleep 1s

wave_pid=`ps -ef | grep $pid | grep -v grep`
# echo $wave_pid
if [ -n "$wave_pid" ] ; then
    echo "kill $pid failed"
    sed -i "s/binlog.offset.flush.strategy=sync/binlog.offset.flush.strategy=async/g" $base/conf/realtime.properties
    exit 1
fi

/bin/rm -f $base/bin/wave.pid
