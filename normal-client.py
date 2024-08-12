#!/usr/bin/python3
import redis
import time
import datetime
import os
import sys
# Redis cluster seed hosts
MYHOSTS = "myhosts"

# init conn
def init_conn(ipport:str, password='') -> redis.Redis:
    if not os.path.exists(MYHOSTS):
        return None
    
    conn = None
    ip, port = ipport.split(':')
    if len(password) ==0:
        conn = redis.Redis(host=ip, port=int(port))
    else:
        conn = redis.Redis(host=ip, port=int(port), password=password)

    return conn

# client run
# set key: test<num> to <num> infinitely
def redis_run(conn: redis.Redis):
    cnt = 1
    while True:
        try:
            key = f"test{cnt}"
            conn.set(key, cnt)
            value = conn.get(key)
            print(f"{key}: {value}, {datetime.datetime.now()}")
            cnt += 1
        except Exception as e:
            print(f"fail to write key: {key} at {datetime.datetime.now()}, wait and retry ... {e}")
        time.sleep(1)

def redis_test(conn: redis.Redis, limit: int):
    i = 1
    w = 0
    while i <= limit:
        if i % 50 == 0:
            print(f"i = {i}, {round(i/limit*100, 2)}%")
        
        try:
            key = f"test{i}"
            value = conn.get(key)
            if value is None:
                print(f"key: {key} value is None")
                w += 1
            elif int(value) != i:
                print(f"key: {key} value is {value}, not {i}")
                w += 1
            i += 1
        except Exception as e:
            print(f"fail to read key: {key} at {datetime.datatime.now()}, wati and retry ... {e}")
            time.sleep(1)
        time.sleep(0.1)

    print(f"i = {i-1}, 100%")
    print(f"checked from test1 to test{limit}, wrong numbers: {w}")

# helper message
HELPER_MSG = '''
run: write and read keys for testing
    run ip:port
    run ip:port <password>
test: test key-existance, from test1 to test<num>
    test <num> ip:port
    test <num> ip:port <password>
'''
if __name__ == "__main__":
    c = None
    if len(sys.argv) <= 2:
        print(HELPER_MSG)
        exit()
    if sys.argv[1] == "run":
        if len(sys.argv) == 3:
            c = init_conn(sys.argv[2])
        else:
            c = init_conn(sys.argv[2], sys.argv[3])
        redis_run(c)
    elif sys.argv[1] == "test":
        if len(sys.argv) == 4:
            c = init_conn(sys.argv[3])
        elif len(sys.argv) > 4:
            c = init_conn(sys.argv[3], sys.argv[4])
        else:
            print("wrong number of args")
            exit()
        redis_test(c, int(sys.argv[2]))
    else:
        print(HELPER_MSG)