#!/bin/bash

current=0
threshold=$1

function kill_program()
{
    pid=`ps ax|grep $1|grep -v grep|awk '{print $1}'`
    echo "pid is ${pid}"

    while [ -n "${pid}" ];
    do
        kill -TERM $pid

        sleep 1

        pid=`ps ax|grep $1|grep -v grep|awk '{print $1}'`
        echo "pid is ${pid}"
    done
}

while :
do
    current=`taos -s 'select last_row(mem_taosd) from log.dn1'|tail -3|head -1|awk '{print $1}'|head -1`

    echo "threshold is ${threshold}, current is ${current}"

    if (( $(echo "$current > $threshold" | bc -l) )); then
        kill_program $2
        kill_program $3
        break;
    else
        echo "sleep 30 seconds and check again."
        sleep 30
    fi
done

