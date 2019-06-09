#!/usr/bin/python
# -*- coding: UTF-8 -*-

import ConfigParser
from optparse import OptionParser
import happybase
from datetime import datetime
import re
import json
import sys
from ctypes import cdll
import ctypes
import pydoop.hdfs as hdfs

#加载配置信息
config = ConfigParser.ConfigParser()
config.readfp(open('/root/eventdb/gx/evtQuery/config.ini','r'))
#hbase信息
hbase_host = config.get('DB','host')
table_prefix = config.get('DB','table_prefix')
table_prefix_separator = config.get('DB','table_prefix_separator')
hbase_timeout = int(config.get('DB','timeout'))
#hdfs信息
hdfs_host = config.get('HDFS','host')
hdfs_port = int(config.get('HDFS','port'))
hdfs_user = config.get('HDFS', 'user')
eventdb_dir = config.get('HDFS','eventdb_dir')
#加载TypeSer.so
double_lib_path = config.get('DOUBLE','double_lib_path')
c_lib = cdll.LoadLibrary(double_lib_path+'/TypeSer.so')
c_lib.DoubleS.restype = ctypes.c_char_p
c_lib.DoubleS.argtypes=[ctypes.c_double]
c_lib.IntS.argtypes=[ctypes.c_int]
c_lib.IntS.restype = ctypes.c_char_p
#连接hdfs
eventdb_fs = hdfs.hdfs(host = hdfs_host, port = hdfs_port, user = hdfs_user)
#连接hbase
eventdb_conn = happybase.Connection(host = hbase_host, table_prefix = table_prefix, table_prefix_separator = table_prefix_separator, timeout = hbase_timeout)
eventdb_conn.open()

#从hfile中读取json对象
def get_query_jsonobj(dataarr, tableName):
	objarr = []
	if len(dataarr) == 0:
		return objarr
	run = dataarr[0]['data:run']
	datafile = eventdb_dir + '/' + tableName + '/data/' + run + '.data'

	with eventdb_fs.open_file(datafile) as f:
		for data in dataarr:
			offset = int(data['data:offset'])
			length = int(data['data:length'])
			f.seek(offset)
			obj = json.loads(f.read(length))
			objarr.append(obj)
	
	return objarr

#同一个run同一个query的不同行
def concat_jsonobj(queryobj, obj):
	for key in obj:
		if key in queryobj:
			queryobj[key] = queryobj[key] + obj[key]
		else:
			queryobj[key] = obj[key]

#queryobj与操作
def and_query_jsonobj(queryobj, tmpobj):
	tmpquery = {}
	for key in tmpobj:
		if key in queryobj:
			tmparr = queryobj[key] & tmpobj[key]
			if len(tmparr) != 0:
				tmpquery[key] = tmparr
	return tmpquery
			

#queryobj或操作
def or_query_jsonobj(queryobj, tmpobj):
	for key in tmpobj:
		if key in queryobj:
			queryobj[key] = queryobj[key] | tmpobj[key]
		else:
			queryobj[key] = tmpobj[key]


#查询
def QueryEventFromHBase(version_str, run_id_list, query_str_org):
	#开始统计查询时间
	start_time = datetime.now()
	#version_str是数据库表名
	table = eventdb_conn.table(version_str)

	#处理查询条件
	query_str_org = "|" + query_str_org.replace("&&","$&").replace("||","$|")
	query_str_list = query_str_org.split("$")
	pattern = re.compile(r'(?P<logic>.*)range\((?P<property>\w*),(?P<lower>(-)?\d*\.?\d+),(?P<upper>(-)?\d*\.?\d+)\)')

	#查询结果
	queryobj = {}
	#待查询run list
	run_no_list = [x for x in run_id_list]
	for run_no in run_no_list:
		#当前run查询结果
		runquery = {}
		for query_str in query_str_list:
			source = query_str.replace(" ","")
			single_range = re.match(pattern,source).groupdict()

			#待查询范围编码，与HBase一致。
			lower_binary = Value2Binary(single_range['lower'])
			upper_binary = Value2Binary(single_range['upper'])

			#生成查询条件，rowkey格式一致。 eg: -8093#NShowes#80000000 			
			query_row_start = (run_no + '#' + single_range['property'] + '#' + lower_binary.__str__()).encode('utf-8')
			query_row_stop = (run_no + '#' + single_range['property'] + '#' + upper_binary.__str__()).encode('utf-8')
			
			#开始扫描查询
			this_query = table.scan(row_start = query_row_start, row_stop = query_row_stop)

			#当前run当前query的结果
			tmpobj = {}
			dataarr = [data for key,data in this_query]
			tmpobjarr = get_query_jsonobj(dataarr, version_str)
			for obj in tmpobjarr:
				concat_jsonobj(tmpobj, obj)
			for key in tmpobj:
				tmpobj[key] = set(tmpobj[key])
			if single_range['logic']=='|':
				or_query_jsonobj(runquery, tmpobj)
			else:
				runquery = and_query_jsonobj(runquery, tmpobj)
		or_query_jsonobj(queryobj, runquery)

	#结束统计查询时间
	stop_time = datetime.now()
	time_cost = (stop_time - start_time).total_seconds()

	count = 0
	for key in queryobj:
		queryobj[key] = list(queryobj[key])
		count += len(queryobj[key])

	return queryobj, count, time_cost

#编码转换
def Value2Binary(Value):
	#判断是否为浮点数
	if Value.find(".") != -1:
		Binary = c_lib.DoubleS((float)(Value))
	else:
		Binary = c_lib.IntS((int)(Value))
	#返回编码结果
	return Binary


#处理命令行参数
def OptionParserFunc():
	usage = "Usage: %prog [option] arg1 arg2 arg3"
	parser = OptionParser(usage)

	parser.add_option("-v", "--version", dest = "version", action = "store", type = "string", help ="version, ex: EventsPadding")
	parser.add_option("-f", "--file", dest = "filename", action = "store", type="string", help = "write data to FILE", metavar = "FILE")
	parser.add_option("-q", "--query", dest = "query_string", action = "store", type="string", help = "type your query condition, ex:'range(totalTrks,2,3)&&range(totalCharged,10,11)'")
	parser.add_option("-r", "--run", dest = "run_id", action = "store", type = "string", help ="run id, ex: -8025, -8002")
	
	(option, args) = parser.parse_args()

	version_str = option.version
	run_id_list = (option.run_id).split(",")
	query_str_org = option.query_string
	output = option.filename

	return version_str, run_id_list, query_str_org, output

#主程序
if __name__ == '__main__':
	version_str, run_id_list, query_str_org, output = OptionParserFunc()

	if(query_str_org is None):
		print("Please check your input option!")
		sys.exit(0)

	event_index_object, event_num, time_cost= QueryEventFromHBase(version_str, run_id_list, query_str_org)
	
	if (event_num == 0):
		print("No Event Retrieved!")
		sys.exit(0)

	print("********************Starting create query result********************")
	print((str)(event_num) + " event(s) retreived in " + time_cost.__str__() + " seconds !")

	f = open(output, 'w')
	f.write(json.dumps(event_index_object))
	f.close()

	print(output.split("/")[-1] + " created!")
	print("********************************************************************")
	eventdb_conn.close()
