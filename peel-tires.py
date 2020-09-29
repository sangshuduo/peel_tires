#! /usr/bin/python3
###################################################################
#           Author: Shudo Sang <sangshuduo@gmail.com>
#           Claim:
#               this program can be distributed
#               under Apache License
#
###################################################################
# Tested on Ubuntu 18.04
###################################################################
# sudo apt install python3-pip
# git clone https://github.com/taosdata/TDengine
# pip3 install multipledispatch
# pip3 install TDengine/src/connector/python/linux/python3/
###################################################################

# -*- coding: utf-8 -*-

import sys
import getopt
import datetime
from multipledispatch import dispatch

import taos


@dispatch(str, str)
def v_print(msg:str, arg:str):
    if verbose:
        print(msg % arg)

@dispatch(str, int)
def v_print(msg:str, arg:int):
    if verbose:
        print(msg % int(arg))

@dispatch(str, int, int)
def v_print(msg:str, arg1:int, arg2:int):
    if verbose:
        print(msg % (int(arg1), int(arg2)))

if __name__ == "__main__":
    verbose = False
    dropDbOnly = False
    numOfDb = 1
    numOfTb = 1
    numOfRec = 10

    opts, args = getopt.gnu_getopt(sys.argv[1:], 'd:t:r:f:pvh', [
        'numofDb', 'numofTb', 'numofRec', 'File=', 'droPdbonly', 'Verbose', 'Help'])
    for key, value in opts:
        if key in ['-h', '--Help']:
            print('')
            print(
                'Peel Tires Run for TDengine')
            print('Author: Shuduo Sang <sangshuduo@gmail.com>')
            print('')

            print('\t-d --numofDb specify number of databases, default is 1')
            print('\t-t --numofTb specify number of tables per database, default is 1')
            print('\t-r --numofRec specify number of records per table, default is 10')
            print('\t-p --droPdbonly drop exist database, number specified by -d')

            print('\t-v --verbose verbose output')
            print('')
            sys.exit(0)

        if key in ['-v', '--Verbose']:
            verbose = True

        if key in ['-p', '--droPdbonly']:
            dropDbOnly = True

        if key in ['-d', '--numofDb']:
            numOfDb = int(value)
            v_print("numOfDb is %d", numOfDb)

        if key in ['-t', '--numofTb']:
            numOfTb = int(value)
            v_print("numOfTb is %d", numOfTb)

        if key in ['-r', '--numofRec']:
            numOfRec = int(value)
            v_print("numOfRec is %d", numOfRec)

        if key in ['-f', '--File']:
            fileOut = value
            v_print("file is %s", fileOut)

    conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos")
    cursor = conn.cursor()
    
    # drop exist databases first
    for i in range(0, numOfDb):
        v_print("will drop database db%d", int(i))
        cursor.execute("drop database if exists db%d" % i)

    if dropDbOnly:
        v_print("drop databases total %d", numOfDb)
        print("Done")
        sys.exit(0)

    # create databases
    for i in range(0, numOfDb):
        v_print("will create database db%d", int(i))
        cursor.execute("create database if not exists db%d" % i)

    # create databases
    if numOfTb > 0:
        for i in range(0, numOfDb):
            cursor.execute("use db%d" % i)
            v_print("will create %d tables for db%d", int(numOfTb), int(i))
            for j in range(0, numOfTb):
                cursor.execute("create table tb%d (ts timestamp, temperature int, humidity float)" % j)

                if numOfRec > 0:
                    start_time = datetime.datetime(2020, 9, 25)
                    time_interval = datetime.timedelta(seconds=60)
                    sqlcmd = ['insert into tb%d values' % j]
                    for row in range(0, numOfRec):
                        start_time += time_interval
                        sqlcmd.append("('%s', %d, %f)" % (start_time, row, row * 1.2))
    
                    affected_rows = cursor.execute(' '.join(sqlcmd))
    
    if verbose:
        for i in range(0, numOfDb):
            cursor.execute("use db%d" % i)
            for j in range(0, numOfTb):
                cursor.execute("select * from tb%d" % j)
                data = cursor.fetchall()
        
                numOfRows = cursor.rowcount
                numOfCols = len(cursor.description)
        
                for row in range(numOfRows):
                    print("Row%d: ts=%s, temperature=%d, humidity=%f" % (row, data[row][0], data[row][1], data[row][2]))
        
    print("done")
