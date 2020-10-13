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
###################################################################

# -*- coding: utf-8 -*-

import sys
import getopt
import datetime
import requests
import json
import random
from multipledispatch import dispatch

import taos


@dispatch(str, str)
def v_print(msg: str, arg: str):
    if verbose:
        print(msg % arg)


@dispatch(str, int)
def v_print(msg: str, arg: int):
    if verbose:
        print(msg % int(arg))


def restful_execute(host: str, port: int, user: str, password: str, cmd: str):
    url = "http://%s:%d/rest/sql" % (host, port)

    resp = requests.post(url, cmd, auth=(user, password))

    v_print("resp status: %d", resp.status_code)

    if verbose:
        v_print(
            "resp text: %s",
            json.dumps(
                resp.json(),
                sort_keys=True,
                indent=2))
    else:
        print("resp: %s" % json.dumps(resp.json()))


if __name__ == "__main__":
    verbose = False
    dropDbOnly = False
    numOfDb = 1
    numOfTb = 1
    numOfStb = 0
    numOfRec = 10
    ieration = 1
    host = "127.0.0.1"
    port = 6041
    user = "root"
    password = "taosdata"

    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], 's:o:u:w:d:b:t:r:i:f:pnvh', [
            'hoSt', 'pOrt', 'User', 'passWord', 'numofDb', 'numofstB',
            'numofTb', 'numofRec', 'Iteration', 'File=', 'droPdbonly',
            'Noverbose', 'Verbose', 'Help'])
    except getopt.GetoptError as err:
        print('ERROR:', err)
        sys.exit(1)

    if bool(opts) is False:
        print('Try `restful-peel-tires.py --Help` for more options.')
        sys.exit(1)

    for key, value in opts:
        if key in ['-h', '--Help']:
            print('')
            print(
                'Peel Tires (RESTful API Version) Run for TDengine')
            print('Author: Shuduo Sang <sangshuduo@gmail.com>')
            print('')

            print('\t-s --hoSt specify host to connect, default is 127.0.0.1')
            print('\t-o --pOrt specify port to connect, default is 6041')
            print('\t-u --User specify user name, default is root')
            print('\t-w --passWord specify password, default is taosdata')
            print('\t-d --numofDb specify number of databases, default is 1')
            print(
                '\t-b --numofStb specify number of super-tables per database, default is 1')
            print('\t-t --numofTb specify number of tables per database, default is 1')
            print('\t-r --numofRec specify number of records per table, default is 10')
            print(
                '\t-i --Iteration specify number of iteration of insertion, default is 1')
            print('\t-p --droPdbonly drop exist database, number specified by -d')

            print('\t-n --Noverbose for no verbose output')
            print('\t-v --Verbose for verbose output')
            print('')
            sys.exit(0)

        if key in ['-s', '--hoSt']:
            host = value

        if key in ['-o', '--pOrt']:
            port = int(value)

        if key in ['-u', '--User']:
            user = value

        if key in ['-w', '--passWord']:
            password = value

        if key in ['-n', '--Noverbose']:
            verbose = False

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

        if key in ['-b', '--numofstB']:
            numOfStb = int(value)
            v_print("numOfStb is %d", numOfStb)

        if key in ['-r', '--numofRec']:
            numOfRec = int(value)
            v_print("numOfRec is %d", numOfRec)

        if key in ['-i', '--Iteration']:
            iteration = int(value)
            v_print("iteration is %d", iteration)

        if key in ['-f', '--File']:
            fileOut = value
            v_print("file is %s", fileOut)

    v_print("cmd: %s", "show databases")
    restful_execute(
        host,
        port,
        user,
        password,
        "show databases")

    if dropDbOnly:
        v_print("drop databases total %d", numOfDb)

        # drop exist databases first
        for i in range(0, numOfDb):
            v_print("will drop database db%d", int(i))
            restful_execute(
                host,
                port,
                user,
                password,
                "drop database if exists db%d" %
                i)

        print("Done")
        sys.exit(0)

    # CREATE DATABASEs
    for i in range(0, numOfDb):
        v_print("will create database db%d", int(i))
        restful_execute(
            host,
            port,
            user,
            password,
            "CREATE DATABASE if not exists db%d" %
            i)
        restful_execute(host, port, user, password, "USE db%d" % i)

    if numOfStb > 0:
        for i in range(0, numOfStb):
            restful_execute(
                host,
                port,
                user,
                password,
                "CREATE TABLE if not exists st%d (ts timestamp, value float) TAGS (uuid binary(50))" %
                i)

            for iterator in range(0, iteration):
                v_print("Iteration %d:", iteration)

                sqlCmd = ['INSERT INTO']
                if numOfRec > 0:
                    for row in range(0, numOfRec):
                        uuid = ''
                        for j in range(0, 50):
                            uuid_itr = random.randint(0, 9)
                            uuid = uuid + ("%s" % uuid_itr)

                        v_print("uuid is: %s", uuid)
                        start_time = datetime.datetime(2020, 9, 25)
                        sqlCmd.append(
                            "tb_%s USING st%d TAGS('%s') VALUES('%s', %f)" %
                            (uuid, i, uuid, start_time, random.random()))

                    cmd = ' '.join(sqlCmd)
                    v_print("sqlCmd: %s", cmd)
                    restful_execute(host, port, user, password, cmd)

        if verbose:
            for i in range(0, numOfDb):
                restful_execute(host, port, user, password, "use db%d" % i)
                for j in range(0, numOfTb):
                    restful_execute("select count(*) from stb%d" % j)

        print("done")
        sys.exit(0)

    if numOfTb > 0:
        for i in range(0, numOfDb):
            restful_execute(host, port, user, password, "USE db%d" % i)
            for j in range(0, numOfTb):
                restful_execute(
                    host,
                    port,
                    user,
                    password,
                    "create table tb%d (ts timestamp, value float)" %
                    j)

                if numOfRec > 0:
                    start_time = datetime.datetime(2020, 9, 25)
                    time_interval = datetime.timedelta(seconds=60)
                    sqlCmd = ['INSERT INTO tb%d VALUES' % j]
                    for row in range(0, numOfRec):
                        start_time += time_interval
                        sqlCmd.append(
                            "('%s', %f)" %
                            (start_time, row * 1.2))

                    restful_execute(
                        host, port, user, password, ' '.join(sqlCmd))

    if verbose:
        for i in range(0, numOfDb):
            restful_execute(host, port, user, password, "use db%d" % i)
            for j in range(0, numOfTb):
                restful_execute("select count(*) from tb%d" % j)

    print("done")
