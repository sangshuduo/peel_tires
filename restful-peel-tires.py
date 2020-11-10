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
import time
from multiprocessing import Process, Pool
from multipledispatch import dispatch
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED


@dispatch(str, str)
def v_print(msg: str, arg: str):
    if verbose:
        print(msg % arg)


@dispatch(str, int)
def v_print(msg: str, arg: int):
    if verbose:
        print(msg % int(arg))


@dispatch(str, int, int)
def v_print(msg: str, arg1: int, arg2: int):
    if verbose:
        print(msg % (int(arg1), int(arg2)))


@dispatch(str, int, int, int)
def v_print(msg: str, arg1: int, arg2: int, arg3: int):
    if verbose:
        print(msg % (int(arg1), int(arg2), int(arg3)))


@dispatch(str, int, int, int, int)
def v_print(msg: str, arg1: int, arg2: int, arg3: int, arg4: int):
    if verbose:
        print(msg % (int(arg1), int(arg2), int(arg3), int(arg4)))


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


def insert_data(processes: int):
    pool = Pool(processes)

    begin = 0
    end = 0

    quotient = iteration // processes
    if quotient < 1:
        processes = iteration
        quotient = 1

    remainder = iteration % processes
    v_print(
        "Iteration %d, quotient %d, remainder %d",
        iteration,
        quotient,
        remainder)

    for i in range(processes):
        begin = end

        if i < remainder:
            end = begin + quotient + 1
        else:
            end = begin + quotient

        v_print("Process %d from %d to %d", i, begin, end)
        pool.apply_async(insert_data_process, args=(i, begin, end, ))

    pool.close()
    pool.join()


def create_stb():
    for i in range(0, numOfStb):
        restful_execute(
            host,
            port,
            user,
            password,
            "CREATE TABLE IF NOT EXISTS st%d (ts timestamp, value float) TAGS (uuid binary(50))" %
            i)


def create_database():
    for i in range(0, numOfDb):
        v_print("will create database db%d", int(i))
        restful_execute(
            host,
            port,
            user,
            password,
            "CREATE DATABASE IF NOT EXISTS db%d" %
            i)


def drop_database():
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


def insert_func(arg: int):
    v_print("Thread arg: %d", arg)

    # generate uuid
    uuid_int = random.randint(0, numOfTb + 1)
    uuid = "%s" % uuid_int
    v_print("uuid is: %s", uuid)

    v_print("numOfRec %d:", numOfRec)
    if numOfRec > 0:
        sqlCmd = ['INSERT INTO']

        for row in range(0, numOfRec):

            try:
                start_time = datetime.datetime(2020, 9, 25)
                sqlCmd.append(
                    "%s.tb_%s USING %s.st%d TAGS('%s') VALUES('%s', %f)" %
                    (current_db,
                     uuid,
                     current_db,
                     numOfStb -
                     1,
                     uuid,
                     start_time +
                     datetime.timedelta(
                         seconds=random.randint(
                             0,
                             1000)),
                        random.random()))
            except Exception as e:
                print("Error: %s" % e.args[0])

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


def create_tb_no_stb():
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


def insert_data_process(i: int, begin: int, end: int):
    tasks = end - begin
    v_print("Process %d from %d to %d, tasks %d", i, begin, end, tasks)

    with ThreadPoolExecutor(max_workers=threads) as executor:
        workers = []

        for i in range(begin, end):
            workers.append(executor.submit(insert_func, i))
            time.sleep(1)

    for j in range(0, threads):
        wait(worker[j], return_when=ALL_COMPLETED)


if __name__ == "__main__":

    verbose = False
    dropDbOnly = False
    numOfDb = 1
    batch = 1
    numOfTb = 1
    numOfStb = 0
    numOfRec = 10
    ieration = 1
    host = "127.0.0.1"
    oneMoreHost = "NotSupported"
    port = 6041
    user = "root"
    defaultPass = "taosdata"
    iteration = 1
    processes = 1
    threads = 1

    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:],
                                       's:m:o:u:w:d:b:c:t:r:i:f:P:T:pnvh',
                                       [
            'hoSt', 'one-More-host', 'pOrt', 'User',
            'passWord', 'numofDb', 'numofstB',
            'batCh', 'numofTb', 'numofRec', 'Iteration', 'File=',
            'Processes', 'Threads',
            'droPdbonly', 'Nobverbose', 'Verbose', 'Help'
        ])
    except getopt.GetoptError as err:
        print('ERROR:', err)
        print('Try `restful-peel-tires.py --Help` for more options.')
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
            print('\t-c --batCh specify number of batch for commands execution, default is 1')
            print('\t-t --numofTb specify number of tables per database, default is 1')
            print('\t-r --numofRec specify number of records per table, default is 10')
            print(
                '\t-i --Iteration specify number of iteration of insertion, default is 1')
            print('\t-P --Processes specify number of processes')
            print('\t-T --Threads specify number of threads')
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
            password = defaultPass

        if key in ['-n', '--Noverbose']:
            verbose = False

        if key in ['-v', '--Verbose']:
            verbose = True

        if key in ['-P', '--Processes']:
            processes = int(value)
            if processes < 1:
                print("FATAL: number of processes must be larger than 0")
                sys.exit(1)

        if key in ['-T', '--Threads']:
            threads = int(value)
            if threads < 1:
                print("FATAL: number of threads must be larger than 0")
                sys.exit(1)

        if key in ['-p', '--droPdbonly']:
            dropDbOnly = True

        if key in ['-d', '--numofDb']:
            numOfDb = int(value)
            v_print("numOfDb is %d", numOfDb)

        if key in ['-c', '--batCh']:
            batch = int(value)

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

    start_time = time.time()

    if dropDbOnly:
        drop_database()
        print("Drop Database done.")
        sys.exit(0)

    # create databases
    current_db = "db"
    if numOfDb > 0:
        create_database()

    # use last database
    current_db = "db%d" % (numOfDb - 1)
    restful_execute(host, port, user, password, "USE %s" % current_db)

    if numOfStb > 0:
        create_stb()
        insert_data(processes)

        if verbose:
            for i in range(0, numOfDb):
                for j in range(0, numOfStb):
                    restful_execute(host, port, user, password,
                                    "SELECT COUNT(*) FROM db%d.st%d" % (i, j,))

        print("done")
        end_time = time.time()
        print(
            "Total time consumed {} seconds.".format(
                (end_time - start_time)))

        sys.exit(0)

    if numOfTb > 0:
        create_tb_no_stb()

        if verbose:
            for i in range(0, numOfDb):
                restful_execute(host, port, user, password, "USE db%d" % i)
                for j in range(0, numOfTb):
                    restful_execute(host, port, user, password,
                                    "SELECT COUNT(*) FROM tb%d" % (j,))

    print("done")
    end_time = time.time()
    print("Total time consumed {} seconds.".format((end_time - start_time)))
