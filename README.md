Peel tires run for TDengine

[![GuardRails badge](https://api.guardrails.io/v2/badges/sangshuduo/peel_tires.svg?token=c47be6225bf670a191fdac8c33a6b928541d3e4d7fd95ec38bd68bac30be1db5&provider=github)](https://dashboard.guardrails.io/gh/sangshuduo/48940)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/sangshuduo/peel_tires.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/sangshuduo/peel_tires/context:python)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/sangshuduo/peel_tires.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/sangshuduo/peel_tires/alerts/)

# peel-tires.py
Peel Tires Run for TDengine

Author: Shuduo Sang <sangshuduo@gmail.com>

Usage: peel-tires.py [Option...]

    -d --numofDb specify number of databases, default is 1
    -t --numofTb specify number of tables per database, default is 1
    -r --numofRec specify number of records per table, default is 10
    -p --droPdbonly drop exist database, number specified by -d
    -v --verbose verbose output


# restful-peel-tires.py
Peel Tires (RESTful API Version) Run for TDengine

Author: Shuduo Sang <sangshuduo@gmail.com>

	-s --hoSt specify host to connect, default is 127.0.0.1
	-m --one-More-host specify one more host to connect, default is not supported
	-o --pOrt specify port to connect, default is 6041
	-u --User specify user name, default is root
	-w --passWord specify password, default is taosdata
	-d --numofDb specify number of databases, default is 1
	-b --numofStb specify number of super-tables per database, default is 1
	-t --numofTb specify number of tables per database, default is 1
	-r --numofRec specify number of records per table, default is 10
	-i --Iteration specify number of iteration of insertion, default is 1
	-p --droPdbonly drop exist database, number specified by -d
	-n --Noverbose for no verbose output
	-v --Verbose for verbose output
