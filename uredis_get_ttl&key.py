#!/usr/bin/python2
import sys
import os
import redis
import time
import datetime

BigNum=10000
string_keys=[]
hash_keys=[]
list_keys=[]
set_keys=[]
zset_keys=[]

def scan_string(source):
    keys_count = len(string_keys)
    for key in string_keys:
        result=source.ttl(key)
        if result is not None:
            #pos = key.index(".")
            #meta_key = key[]
            #meta_key = key[pos+1:]
            key_file.write(key)
            key_file.write("\n")


def scan_hash(source):
    #print "Begin Import Hash Type"
    keys_count = len(hash_keys)
    #print "Hash Key Count is:",keys_count
    for key in hash_keys:
        result=source.ttl(key)
        if result is not None:
            #pos = key.index(".")
            #meta_key = key[]
            #meta_key = key[pos+1:]
            key_file.write(key)
            key_file.write("\n")

def scan_set(source):
    #print "Begin Import Set Type"
    keys_count = len(set_keys)
    #print "Set Key Count is:",keys_count
    for key in set_keys:
        result=source.ttl(key)
        if result is not None:
            #pos = key.index(".")
            #meta_key = key[pos+1:]
            #key_file.write(meta_key)
            key_file.write("\n")

def scan_zset(source):
    #print "Begin Import ZSet Type"
    keys_count = len(zset_keys)
    #print "ZSet Key Count is:",keys_count
    for key in zset_keys:
        result=source.ttl(key)
        if result is not None:
            #pos = key.index(".")
            #meta_key = key[pos+1:]
            key_file.write(key)
            key_file.write("\n")

def scan_list(source):
    #print "Begin Import List Type"
    keys_count = len(list_keys)
    #print "List Key Count is:",keys_count
    for key in list_keys:
        result=source.ttl(key)
        if result is not None:
            #pos = key.index(".")
            #meta_key = key[pos+1:]
            key_file.write(key)
            key_file.write("\n")

def read_type_keys(source, ScanIndex):
    ScanIndex, keys = source.execute_command('scan', ScanIndex, "count",200000)
    keys_count = len(keys)
    #print "Key Count is:",keys_count
    pipe = source.pipeline(transaction=False)
    #for key in keys:
    index=0
    pipe_size=5000
    while index < keys_count:
        old_index=index
        num=0
        while (index < keys_count) and (num < pipe_size):
            pipe.type(keys[index])
            index +=1
            num +=1
        results=pipe.execute()
        for type in results:
            if type == "string":
                string_keys.append(keys[old_index])
            elif type == "list":
                list_keys.append(keys[old_index])
            elif type == "hash":
                hash_keys.append(keys[old_index])
            elif type == "set":
                set_keys.append(keys[old_index])
            elif type == "zset":
                zset_keys.append(keys[old_index])
            else :
                print "no key"
                #print keys[old_index]," is not find when TYPE"
            old_index +=1
    return ScanIndex

if __name__=='__main__':
    argc = len(sys.argv)
    if argc != 3:
        print "usage: %s sourceIP sourcePort" % (sys.argv[0])
        exit(1)
    SrcIP = sys.argv[1]
    SrcPort = int(sys.argv[2])

    start=datetime.datetime.now()
    source=redis.Redis(host=SrcIP,port=SrcPort)

    index=0
    times=1
    strNum=0
    setNum=0
    zsetNum=0
    listNum=0
    hashNum=0
    print "begin scan key"
    print "..."
    key_file_name = "/root/key_" + SrcIP + ":" + str(SrcPort) + ".txt"
    print key_file_name
    key_file = open(key_file_name, 'w')
    while index != '0' or times == 1:
        first = False
        times += 1
        index=read_type_keys(source, index)
        strNum += len(string_keys)
        setNum += len(set_keys)
        zsetNum += len(zset_keys)
        listNum += len(list_keys)
        hashNum += len(hash_keys)

        scan_string(source)
        scan_hash(source)
        scan_list(source)
        scan_set(source)
        scan_zset(source)
        string_keys=[]
        hash_keys=[]
        list_keys=[]
        set_keys=[]
        zset_keys=[]

    stop=datetime.datetime.now()
    diff=stop-start
    key_file.close()
    print "..."
    print "Finish, token time:",str(diff)
                                               
