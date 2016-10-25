#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time,datetime,os,sys
import random
from rediscluster import StrictRedisCluster
sys.path.append("..")
import config.config as config

class load_class:
    def __init__(self):
	self.rc = StrictRedisCluster(startup_nodes=config.startup_nodes)
	self.key_num = 1
    def _string(self,num,string_value_len,thread_num):
	key_num = self.key_num
	while key_num <= num:
           key_name = 'string_r_test_' + str(thread_num) + ':00' + str(key_num)
           self.rc.set(key_name,''.join(map(lambda xx:(hex(ord(xx))[2:]),os.urandom(int(string_value_len)/2))))
           key_num += 1
    def _hash(self,num,hash_value_len,thread_num,filed_num):
	key_num = self.key_num
	while key_num <= num:
	    key_name = 'hash_r_test_' + str(thread_num) + ':00' + str(key_num)
	    _file_num = 1
	    filed_set = {}
	    while _file_num <= filed_num:
	       key_filed = str(_file_num) + ':value_test'
	       filed_set[key_filed] = ''.join(map(lambda xx:(hex(ord(xx))[2:]),os.urandom(hash_value_len/2)))
	       _file_num += 1
	    self.rc.hmset(key_name,filed_set)
	    key_num += 1
    def _set(self,num,set_value_len,thread_num,value_num):
	key_num = self.key_num
        while key_num <= num:
            key_name = 'set_r_test_' + str(thread_num) + ':00' + str(key_num)
            _value_num = 1
            value_set = []
            while _value_num <= value_num:
               self.rc.sadd(key_name,''.join(map(lambda xx:(hex(ord(xx))[2:]),os.urandom(set_value_len/2))))
               _value_num += 1
            key_num += 1	
    def _list(self,num,list_value_len,thread_num,value_num):
	key_num = self.key_num
        while key_num <= num:
            key_name = 'list_r_test_' + str(thread_num) + ':00' + str(key_num)
            _value_num = 1
            value_set = []
            while _value_num <= value_num:
		self.rc.lpush(key_name,''.join(map(lambda xx:(hex(ord(xx))[2:]),os.urandom(list_value_len/2))))
		_value_num += 1
            key_num += 1
