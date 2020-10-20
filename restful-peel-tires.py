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

    if verbose:
        v_print("cmd: %s", cmd)

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
    oneMoreHost = "NotSupported"
    port = 6041
    user = "root"
    defPass = "taosdata"

    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:],
                                       's:m:o:u:w:d:b:t:r:i:f:pnvh',
                                       [
            'hoSt', 'one-More-host', 'pOrt', 'User',
            'passWord', 'numofDb', 'numofstB',
            'numofTb', 'numofRec', 'Iteration', 'File=', 'droPdbonly',
            'Nobverbose', 'Verbose', 'Help'
        ])
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
            print(
                '\t-m --one-More-host specify one more host to connect, default is not supported')
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

        if key in ['-m', '--one-More-host']:
            oneMoreHost = value

        if key in ['-o', '--pOrt']:
            port = int(value)

        if key in ['-u', '--User']:
            user = value

        if key in ['-w', '--passWord']:
            password = value
        else:
            password = defPass

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

    restful_execute(
        host,
        port,
        user,
        password,
        "SHOW DATABASES")

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
                "DROP DATABASE IF EXISTS db%d" %
                i)

        print("done")
        sys.exit(0)

    # create databases
    current_db = "db"

    for i in range(0, numOfDb):
        v_print("will create database db%d", int(i))
        restful_execute(
            host,
            port,
            user,
            password,
            "CREATE DATABASE IF NOT EXISTS db%d" %
            i)
        current_db = "db%d" % i
        restful_execute(host, port, user, password, "USE %s" % current_db)

    if numOfStb > 0:
        for i in range(0, numOfStb):
            restful_execute(
                host,
                port,
                user,
                password,
                "CREATE TABLE IF NOT EXISTS st%d (ts timestamp, value float) TAGS (uuid binary(50))" %
                i)

            for iterator in range(0, iteration):
                v_print("Iteration %d:", iteration)

                if numOfTb > 0:
                    v_print("numOfTb %d:", numOfTb)

                    for table in range(0, numOfTb):

                        # generate uuid
                        uuid = ''
                        for j in range(0, 50):
                            uuid_itr = random.randint(0, 9)
                            uuid = uuid + ("%s" % uuid_itr)
                        v_print("uuid is: %s", uuid)

                        v_print("numOfRec %d:", numOfRec)
                        if numOfRec > 0:
                            sqlCmd = ['INSERT INTO']

                            for row in range(0, numOfRec):

                                start_time = datetime.datetime(2020, 9, 25)
                                sqlCmd.append(
                                    "%s.tb_%s USING %s.st%d TAGS('%s') VALUES('%s', %f)" %
                                    (current_db, uuid, current_db, i, uuid,
                                     start_time + datetime.timedelta(seconds=row),
                                     random.random()))

                            cmd = ' '.join(sqlCmd)
                            v_print("sqlCmd: %s", cmd)

                            if oneMoreHost is not "NotSuppored" and random.randint(
                                    0, 1) is 1:
                                v_print("%s", "Send to second host")
                                restful_execute(
                                    oneMoreHost, port, user, password, cmd)
                            else:
                                v_print("%s", "Send to first host")
                                restful_execute(
                                    host, port, user, password, cmd)

        if verbose:
            for i in range(0, numOfDb):
                for j in range(0, numOfStb):
                    restful_execute(host, port, user, password,
                                    "SELECT COUNT(*) FROM db%d.st%d" % (i, j,))

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
                    "CREATE TABLE tb%d (ts timestamp, value float)" %
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
            restful_execute(host, port, user, password, "USE db%d" % i)
            for j in range(0, numOfTb):
                restful_execute(host, port, user, password,
                                "SELECT COUNT(*) FROM tb%d" % (j,))

    print("done")
