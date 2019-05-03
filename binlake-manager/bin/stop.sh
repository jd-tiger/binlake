#!/bin/sh
cd ../
base=`pwd`
# echo $base
pid_file=$base/bin/tower.pid
if [ ! -f "$pid_file" ] ; then
    echo "tower server is not running. exists"
    exit 1
fi

pid=$(cat $pid_file)

echo -e "stopping tower $pid ... "
kill -9 $pid

sleep 1s

tower_pid=`ps -ef | grep $pid | grep -v grep`
# echo $tower_pid
if [ -n "$tower_pid" ] ; then
    echo "kill $pid failed"
    exit 1
fi

mv $base/bin/tower.pid /tmp/
