#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys
from multiprocessing import Process,Pool
sys.path.append("..")
import config.config as config
import load_class



def _load(num,key_type,**kwargs):
    _num = int(num/int(kwargs['thread_num']))
    _run_load = load_class.load_class()
    _thread_num = 1
    if key_type == 'string':
	while _thread_num <= kwargs['thread_num']:
	    try:
	       p = Process(target=_run_load._string, args=(_num,kwargs['string_value_len'],_thread_num,))
	    except:
	       print "POOL ERROR"
	    _thread_num += 1
	    p.start()
    elif key_type == "hash":
	while _thread_num <= kwargs['thread_num']:
	    try:
               p = Process(target=_run_load._hash, args=(_num,kwargs['file_value_len'],_thread_num,kwargs['file_num']))
	    except:
	       print "POOL ERROR"
            _thread_num += 1
	    p.start()
    elif key_type == "set":
	while _thread_num <= kwargs['thread_num']:
	    try:
               p = Process(target=_run_load._set, args=(_num,kwargs['set_value_len'],_thread_num,kwargs['value_num']))
            except:
               print "POOL ERROR"
            _thread_num += 1
            p.start()
    elif key_type == "list":
	while _thread_num <= kwargs['thread_num']:
            try:
               p = Process(target=_run_load._list, args=(_num,kwargs['list_value_len'],_thread_num,kwargs['value_num']))
            except:
               print "POOL ERROR"
            _thread_num += 1
            p.start()
    else:
	print "key_type input error,please execute -h!!"


