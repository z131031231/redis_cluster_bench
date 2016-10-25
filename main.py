#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time,datetime,os,sys
#import random,getopt
#from rediscluster import StrictRedisCluster
from lib_im.load import _load
from lib_im.client import run
from multiprocessing import Process,Queue, Pool
import getopt

class modelclass:
    def __init__(self):
		pass
    def judge_select(self,opts,__usage__):
	if '-k' in opts and opts['-t'] != 'string' and opts['-t'] != 'set':
		run(mode=opts['-m'],key_type=opts['-t'],_time=int(opts['-d']),_thread=int(opts['-c']),key_num=int(opts['-n']),filed_num=int(opts['-k']),partition=int(opts['-C']))
	elif '-k' not in opts and (opts['-t'] == 'string' or opts['-t'] == 'set'):
        	run(mode=opts['-m'],key_type=opts['-t'],_time=int(opts['-d']),_thread=int(opts['-c']),key_num=int(opts['-n']),partition=int(opts['-C']))
        else:
        	print __usage__
    def judge_mixed(self,opts,__usage__):
	if '-p' not in opts:
		opts['-p'] = 0.5
	if opts['-t'] == 'string':
		run(mode=opts['-m'],key_type=opts['-t'],_time=int(opts['-d']),_thread=int(opts['-c']),key_num=int(opts['-n']),partition=int(opts['-C']),percent=float(opts['-p']))
	elif opts['-t'] == 'hash' or opts['-t'] == 'set' or opts['-t'] == 'list':
		run(mode=opts['-m'],key_type=opts['-t'],_time=int(opts['-d']),_thread=int(opts['-c']),key_num=int(opts['-n']),filed_num=int(opts['-k']),partition=int(opts['-C']),percent=float(opts['-p']))
	else:
		print __usage__
	   

if __name__ == "__main__":
    __usage__ = """
	Usage:
  	simple [-t] [-n] [-c] [-d] [-i] [-m] [-p] [-h] [-l] [-k] [-C]
	Options:
  		-t [type]          	set key type for test: 
					string、hash、list、set [default: string]
  		-n [number]        	The number of key 
                -l [value_length]	Value length
		-k			hash、list、set key value number
  		-c [thread]        	Pressure measurement process [default: 1]
  		-d [time]          	The total time pressure test 
  		-i [interval-time]      The time interval of data returned
  		-m [mode]     		pressure mode:
					load、select、mixed 
  		-p [percent]          	Type the percentage 
					float num[defalut: 0.5]					
  		-h [help]       	show version and exit
		-C [thread]		When test, tell me how many threads when loading the data using the load, I good location query 
					because different threads are different partitions
	examples [load]:
		python main.py -m load -t hash -n 100 -k 10 -l 50
	examples [mixed]:
		python main.py -m mixed -t hash -n 100 -k 10 -p 30 -c 10 -d 60 -i 5
	    """
    _opts,args = getopt.getopt(sys.argv[1:],"ht:n:l:k:c:d:i:m:p:C:")
    opts = {}
    for op,ts in _opts:
        opts[op] = ts
    if '-h' in opts:
        print __usage__
	exit()
    if '-c' not in opts:
	opts['-c'] = 1
    #try:
    if '-m' in opts:
       if opts['-m'] == 'load':
	  if opts['-t'] == 'string':
	     _load(num=int(opts['-n']),key_type=opts['-t'],string_value_len=int(opts['-l']),thread_num=int(opts['-c']))
          elif opts['-t'] == 'hash':
	     _load(num=int(opts['-n']),key_type=opts['-t'],file_num=int(opts['-k']),file_value_len=int(opts['-l']),thread_num=int(opts['-c']))
	  elif opts['-t'] == 'list':
	     _load(num=int(opts['-n']),key_type=opts['-t'],value_num=int(opts['-k']),list_value_len=int(opts['-l']),thread_num=int(opts['-c']))
	  elif opts['-t'] == 'set':
	     _load(num=int(opts['-n']),key_type=opts['-t'],value_num=int(opts['-k']),set_value_len=int(opts['-l']),thread_num=int(opts['-c']))
	  else:
	     print  __usage__
       elif opts['-m'] == 'select':
	    _modelclass = modelclass()
	    _modelclass.judge_select(opts,__usage__)
       elif opts['-m'] == 'mixed':
            _modelclass = modelclass()
            _modelclass.judge_mixed(opts,__usage__)
       else:
            print __usage__


