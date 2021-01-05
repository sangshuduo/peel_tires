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
import requests
import json
import random
import time
import datetime
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


@dispatch(str, str, int)
def v_print(msg: str, arg1: str, arg2: int):
    if verbose:
        print(msg % (arg1, int(arg2)))

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

    quotient = numOfTb // processes
    if quotient < 1:
        processes = numOfTb
        quotient = 1

    remainder = numOfTb % processes
    v_print(
        "num of tables: %d, quotient: %d, remainder: %d",
        numOfTb,
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


def create_databases():
    for i in range(0, numOfDb):
        v_print("will create database db%d", int(i))
        restful_execute(
            host,
            port,
            user,
            password,
            "CREATE DATABASE IF NOT EXISTS db%d" %
            i)


def drop_databases():
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
        row = 0
        while row < numOfRec:
            v_print("row: %d", row)
            sqlCmd = ['INSERT INTO ']
            try:
                sqlCmd.append(
                    "%s.tb%s " % (current_db, arg))

                if (numOfStb > 0 and autosubtable == True):
                    sqlCmd.append("USING %s.st%d TAGS('%s') " %
                        (current_db, numOfStb - 1, uuid))

                start_time = datetime.datetime(2020, 9, 25) + datetime.timedelta(seconds = row)

                sqlCmd.append("VALUES ")
                for batchIter in range(0, batch):
                    sqlCmd.append("('%s', %f) " %
                     (start_time +
                     datetime.timedelta(
                         milliseconds=batchIter),
                        random.random()))
                    row = row + 1
                    if row >= numOfRec:
                        v_print("BREAK, row: %d numOfRec:%d", row, numOfRec)
                        break;

            except Exception as e:
                print("Error: %s" % e.args[0])

            cmd = ' '.join(sqlCmd)

            if measure:
                exec_start_time = datetime.datetime.now()

            if oneMoreHost != "NotSupported" and random.randint(
                    0, 1) == 1:
                v_print("%s", "Send to second host")
                restful_execute(
                    oneMoreHost, port, user, password, cmd)
            else:
                v_print("%s", "Send to first host")
                restful_execute(
                    host, port, user, password, cmd)

            if measure:
                exec_end_time = datetime.datetime.now()
                exec_delta = exec_end_time - exec_start_time
                print("%s, %d" % (time.strftime('%X'), exec_delta.microseconds))

            v_print("cmd: %s, length:%d", cmd, len(cmd))

def create_tb_using_stb():
    # TODO:
    pass

def create_tb():
    v_print("create_tb() numOfTb: %d", numOfTb)
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

def insert_data_process(i: int, begin: int, end: int):
    tasks = end - begin
    v_print("Process:%d table from %d to %d, tasks %d", i, begin, end, tasks)

    with ThreadPoolExecutor(max_workers=threads) as executor:
        workers = []

        for i in range(begin, end):
            workers.append(executor.submit(insert_func, i))
            time.sleep(1)

    for j in range(0, threads):
        wait(worker[j], return_when=ALL_COMPLETED)


if __name__ == "__main__":

    verbose = False
    measure = False
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
    processes = 1
    threads = 1
    insertonly = False
    autosubtable = False

    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:],
                                       's:m:o:u:w:d:b:c:t:r:P:T:pvMxah',
                                       [
            'hoSt', 'one-More-host', 'pOrt', 'User',
            'passWord', 'numofDb', 'numofstB',
            'batCh', 'numofTb', 'numofRec',
            'Processes', 'Threads',
            'droPdbonly', 'Verbose', 'Measure',
            'Autosubtable', 'insertonLy', 'help'
        ])
    except getopt.GetoptError as err:
        print('ERROR:', err)
        print('Try `restful-peel-tires.py --help` for more options.')
        sys.exit(1)

    if bool(opts) is False:
        print('Try `restful-peel-tires.py --help` for more options.')
        sys.exit(1)

    for key, value in opts:
        if key in ['-h', '--help']:
            print('')
            print(
                'Peel Tires (RESTful API Version) Run for TDengine')
            print('Author: Shuduo Sang <sangshuduo@gmail.com>')
            print('')

            print('\t-s --hoSt, specify host to connect, default is 127.0.0.1')
            print(
                '\t-m --one-More-host, specify one more host to connect, default is not supported')
            print('\t-o --pOrt, specify port to connect, default is 6041')
            print('\t-u --User, specify user name, default is root')
            print('\t-w --passWord, specify password, default is taosdata')
            print('\t-d --numofDb, specify number of databases, default is 1')
            print(
                '\t-b --numofStb, specify number of super-tables per database, default is 0')
            print('\t-c --batCh, specify number of batch for commands execution, default is 1')
            print('\t-t --numofTb, specify number of tables per database, default is 1')
            print('\t-r --numofRec, specify number of records per table, default is 10')
            print('\t-P --Processes, specify number of processes')
            print('\t-T --Threads, specify number of threads')
            print('\t-p --droPdbonly, drop exist database, number specified by -d')

            print('\t-v --Verbose, for verbose output')
            print('\t-M --Measure, for performance measure')
            print('\t-A --Autosubtable, automatically create sub-table')
            print('\t-x --insertonLy, insert only, don\'t drop exist database and table')
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

        if key in ['-v', '--Verbose']:
            verbose = True

        if key in ['-A', '--Autosubtable']:
            autosubtable = True

        if key in ['-M', '--Measure']:
            measure = True

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
            if (numOfdb <= 0):
                print("ERROR: wrong number of database given!")
                sys.exit(1)

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

        if key in ['-f', '--File']:
            fileOut = value
            v_print("file is %s", fileOut)

        if key in ['-x', '--insertonLy']:
            insertonly = True
            v_print("insert only: %d", insertonly)

    restful_execute(
        host,
        port,
        user,
        password,
        "SHOW DATABASES")

    if measure:
        start_time = time.time()

    if dropDbOnly:
        drop_databases()
        print("Drop Database done.")
        sys.exit(0)

    # create databases
    current_db = "db"
    if (insertonly == False):
        drop_databases()
    create_databases()

    # use last database
    current_db = "db%d" % (numOfDb - 1)
    restful_execute(host, port, user, password, "USE %s" % current_db)

    if numOfStb > 0:
        create_stb()
        if (autosubtable == False):
            create_tb_using_stb()

        insert_data(processes)

        if verbose:
            for i in range(0, numOfDb):
                for j in range(0, numOfStb):
                    restful_execute(host, port, user, password,
                                    "SELECT COUNT(*) FROM db%d.st%d" % (i, j,))

        print("done")

        if measure:
            end_time = time.time()
            print(
                "Total time consumed {} seconds.".format(
                    (end_time - start_time)))

        sys.exit(0)

    if numOfTb > 0:
        create_tb()
        insert_data(processes)

        if verbose:
            for i in range(0, numOfDb):
                restful_execute(host, port, user, password, "USE db%d" % i)
                for j in range(0, numOfTb):
                    restful_execute(host, port, user, password,
                                    "SELECT COUNT(*) FROM tb%d" % (j,))

    print("done")
    if measure:
        end_time = time.time()
        print("Total time consumed {} seconds.".format((end_time - start_time)))
