#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import time,datetime,os,sys
import random,thread
from rediscluster import StrictRedisCluster
from multiprocessing import Process,Queue, Pool

sys.path.append("..")
import config.config as config

#*******
q = Queue()
q_u = Queue()
#*******

class selectclass:
    def __init__(self):
	self.rc = StrictRedisCluster(startup_nodes=config.startup_nodes)
    def _select(self,key_type,kwargs):
	partition = str(random.randint(1,kwargs['partition']))
	_key_name = key_type + '_r_test_' + partition + ':00' + str(random.randint(1,kwargs['key_num']))
	if key_type == 'string':
		self.rc.get(_key_name)
	elif key_type == 'hash':
	   	_filed = str(random.randint(1,int(kwargs['filed_num']))) + ':value_test'
	   	self.rc.hget(_key_name,_filed)
	elif key_type == 'list':
	   	_num_1 = random.randint(1,int(kwargs['filed_num']))
           	_num_2 = random.randint(1,int(kwargs['filed_num']))
	   	if _num_1 < _num_2:
	      		self.rc.lrange(_key_name,_num_1,_num_2)
	   	else:
	      		self.rc.lrange(_key_name,_num_2,_num_1)
	elif key_type == 'set':
	   	self.rc.srandmember(_key_name)
	else:
		exit()
    def _update(self,key_type,kwargs):
	_value = ''.join(map(lambda xx:(hex(ord(xx))[2:]),os.urandom(random.randint(1,200)/2)))
	partition = str(random.randint(1,kwargs['partition']))
        _key_name = key_type + '_r_test_' + partition + ':00' + str(random.randint(1,kwargs['key_num']))
	if key_type == 'string':
		self.rc.set(_key_name,_value)
	elif key_type == 'hash':
		_filed_name = str(random.randint(1,kwargs['filed_num'])) + ':_value_test'
		self.rc.hset(_key_name,_filed_name,_value)
	elif key_type == 'list':
		_filed_int = random.randint(0,kwargs['filed_num']-1)
		self.rc.lset(_key_name,_filed_int,_value)
	elif key_type == 'set':
		self.rc.spop(_key_name)
		self.rc.sadd(_key_name,_value)
	else:
		exit()
class runclass:
    def __init__(self):
	self._selectclass = selectclass()
	self._now_time = datetime.datetime.now()
    def _run_select(self,key_type,kwargs,_time):
	_num = 0
	while True:
		self._selectclass._select(key_type,kwargs)
		_num += 1
		_now = datetime.datetime.now()
                time_poor = (_now - self._now_time).seconds
                if time_poor > _time:
			break
	q.put(_num)

    def _run_mixed(self,key_type,kwargs,_time):
	_num_r = 0
	_num_u = 0
	while True:
		rand_num = random.random()
		if rand_num < kwargs['percent']:	
			self._selectclass._update(key_type,kwargs)
			_num_u += 1
		else:
			self._selectclass._select(key_type,kwargs)
                	_num_r += 1
                _now = datetime.datetime.now()
                time_poor = (_now - self._now_time).seconds
                if time_poor > _time:
                	break
        q.put(_num_r)
	q_u.put(_num_u)

	

def run(mode,key_type,_time,_thread,**kwargs):
    i = 1
    _runclass = runclass()
    if mode == 'select':
	k = 1
	ops = 0
    	while i <= _thread:
        	p = Process(target=_runclass._run_select,args=(key_type,kwargs,_time,))
		p.start()
		#thread.start_new_thread(_runclass._run_select,(key_type,kwargs,_time,))
		i += 1
	while k <= _thread:
        	ops += int(q.get())
        	k += 1
    elif mode == 'mixed':
	k = 1
	ops_r = 0
	ops_u = 0
	while i <= _thread:
		p = Process(target=_runclass._run_select,args=(key_type,kwargs,_time,))
		p.start()
		#thread.start_new_thread(_runclass._run_select,(key_type,kwargs,_time,))
		i += 1
	while k<= _thread:
		ops_r += int(q.get())
		ops_u += int(q_u.get())
		k += 1
	ops = ops_r + ops_u	

    print "Test type:				%s" % mode
    print "Key type:				%s" % key_type
    print "Key count number:			%s" % str(kwargs['key_num'])
    if 'filed_num' in kwargs:
	print "The number of values:			%s" % str(kwargs['filed_num'])
    print "The execution time:			%s[seconds]" % str(_time)
    print "Concurrent processes:			%s" % str(_thread)
    print "The total number of responses:		%s" % str(ops)
    if 'percent' in kwargs:
	print "update percent:				%s" % str(kwargs['percent']*100) + "%"
	print "update ops:				%s[per/s]" % str(ops_u/_time)
	print "read ops:				%s[per/s]" % str(ops_r/_time)			
    print "The ops :				%s[per/s]" % str(ops/_time)
