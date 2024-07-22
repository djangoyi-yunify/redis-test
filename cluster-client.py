#!/usr/bin/python3
import redis
import time
import datetime
import os
import sys
# Redis cluster seed hosts
MYHOSTS = "myhosts"

# init cluster
def init_cluster(password=''):
    if not os.path.exists(MYHOSTS):
        return None
    
    nodes = []
    try:
        with open(MYHOSTS, 'r') as file:
            for line in file:
                ip, port = line.strip().split()
                nodes.append({"host": ip, "port": int(port)})
    except Exception as e:
        print(f"can't read seed hosts from file: {MYHOSTS}, {e}")
        return None
    
    if len(nodes) == 0:
        print("no seed hosts found")
        return None
    
    cluster = None
    if len(password) ==0:
        cluster = redis.RedisCluster(
            startup_nodes=[
                redis.cluster.ClusterNode(host=node["host"], port=node["port"])
                for node in nodes
            ],
            max_connections=32,
            max_retry=5,
            read_from="slave",
            decode_responses=True)
    else:
        cluster = redis.RedisCluster(
            startup_nodes=[
                redis.cluster.ClusterNode(host=node["host"], port=node["port"])
                for node in nodes
            ],
            max_connections=32,
            max_retry=5,
            read_from="slave",
            decode_responses=True,
            password=password)

    return cluster

# client run
# set key: test<num> to <num> infinitely
def redis_run(cluster: redis.RedisCluster):
    cnt = 1
    while True:
        try:
            key = f"test{cnt}"
            cluster.set(key, cnt)
            value = cluster.get(key)
            print(f"{key}: {value}")
            cnt += 1
        except Exception as e:
            print(f"fail to write key: {key} at {datetime.datetime.now()}, wait and retry ... {e}")
        time.sleep(1)

def redis_test(cluster: redis.RedisCluster, limit: int):
    i = 1
    w = 0
    while i <= limit:
        if i % 50 == 0 and i != 0:
            print(f"i = {i}, {round(i/limit*100, 2)}%")
        
        try:
            key = f"test{i}"
            value = cluster.get(key)
            if value is None:
                w += 1
            elif int(value) != i:
                w += 1
            i += 1
        except Exception as e:
            print(f"ail to read key: {key} at {datetime.datatime.now()}, wati and retry ... {e}")
            time.sleep(1)
        time.sleep(0.1)

    print(f"i = {i-1}, 100%")
    print(f"checked from test1 to test{limit}, wrong numbers: {w}")

# helper message
HELPER_MSG = '''
run: write and read keys for testing
    run
    run <password>
test: test key-existance, from test0 to test<num>
    test <num>
    test <num> <password>
'''
if __name__ == "__main__":
    c = None
    if len(sys.argv) == 1 :
        print(HELPER_MSG)
        exit()
    if sys.argv[1] == "run":
        if len(sys.argv) == 2:
            c = init_cluster()
        else:
            c = init_cluster(sys.argv[2])
        redis_run(c)
    elif sys.argv[1] == "test":
        if len(sys.argv) == 3:
            c = init_cluster()
        elif len(sys.argv) > 3:
            c = init_cluster(sys.argv[3])
        else:
            print("wrong number of args")
            exit()
        redis_test(c, int(sys.argv[2]))
    else:
        print(HELPER_MSG)