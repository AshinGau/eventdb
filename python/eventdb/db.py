import happybase
from events import events
import os, sys
import math
import json
from ctypes import cdll
import ctypes
from datetime import datetime
import re
import pydoop.hdfs as hdfs
import uuid

class runInfo:
	def __init__(self, tableName, host = '192.168.60.64', infoTable = 'runInfo'):
		self.tableName = tableName
		self.conn = happybase.Connection(host)
		self.table = self.conn.table(infoTable)
		self.eventdb = self.conn.table('HEP:' + tableName)
		self.escape = escape()
		self.fs = hdfs.hdfs(host = host, port = 8022, user = 'root')

	def totalEvents(self):
		return self.table.counter_get('totalEvents', 'data:value')

	def volume(self):
		return str(self.totalEvents() / 1024) + 'GB'

	def genInfo(self, dirPath):
		tableName = self.tableName
		for walker in os.walk(dirPath):
			root, dir, files = walker
			if files:
				fileCount = len(files)
				eventCount = 0
				runID = ''
				for file in files:
					cf = os.path.join(root, file)
					with events(cf) as evts:
						eventCount += evts.getEntries()
						runID = evts.getRunNo()
				print 'Finished to analyze run: %s'%runID
				self.table.counter_inc('totalEvents', 'data:value', eventCount)
				self.table.put(tableName + '#' + runID, {
					'data:runID': runID,
					'data:tableName': tableName,
					'data:fileCount': str(fileCount),
					'data:eventCount': str(eventCount)
					})
		self.resetRowCount()

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		self.close()

	def resetRowCount(self):
		rows = self.table.scan(row_prefix = self.tableName, columns = ['data:runID'])
		rowcount = 0
		for key, data in rows:
			rowcount += 1
		self.table.put('runInfo#' + self.tableName, {
			'data:rowCount': str(rowcount)
			})

	def getRowCount(self):
		return int(self.table.row('runInfo#' + self.tableName)['data:rowCount'])

	def page(self, pageIndex = 1, rows = 15, row_start = None):
		rowcount = self.getRowCount()
		pages = float(rowcount) / float(rows)
		pages = int(math.ceil(pages))
		result = []
		if (pageIndex - 1) * rows >= rowcount:
			return rowcount, pages, None
		if row_start:
			rowscan = self.table.scan(row_start = row_start, limit = rows + 1, include_timestamp = True)
			rowscan.next()
			for key, data in rowscan:
				if key.startswith(self.tableName):
					timestamp = 0
					for dk in data:
						timestamp = data[dk][1]
						data[dk] = data[dk][0]
					obj = {}
					obj['tableName'] = data['data:tableName']
					obj['version'] = 'Boss.702p01'
					obj['rowkey'] = key
					obj['eventCount'] = int(data['data:eventCount'])
					obj['time'] = str(datetime.fromtimestamp(timestamp / 1000))
					obj['fileCount'] = int(data['data:fileCount'])
					obj['runID'] = data['data:runID']
					result.append(obj)
		else:
			rowscan = self.table.scan(row_prefix = self.tableName, limit = rows * pageIndex, include_timestamp = True)
			for i in range(int(rows * (pageIndex - 1))):
				rowscan.next()
			for key, data in rowscan:
				if key.startswith(self.tableName):
					timestamp = 0
					for dk in data:
						timestamp = data[dk][1]
						data[dk] = data[dk][0]
					obj = {}
					obj['tableName'] = data['data:tableName']
					obj['version'] = 'Boss.702p01'
					obj['rowkey'] = key
					obj['eventCount'] = int(data['data:eventCount'])
					obj['time'] = str(datetime.fromtimestamp(timestamp / 1000))
					obj['fileCount'] = int(data['data:fileCount'])
					obj['runID'] = data['data:runID']
					result.append(obj)
		tmpobj = {
				'msg': self.tableName,
				'code': 0,
				'count': rowcount,
				'data': result
				}
		#return json.dumps(tmpobj)
		return tmpobj

	def runDetail(self, runID, property):
		rows = self.eventdb.scan(row_prefix = str(runID) + '#' + property, columns = ['data:count'])
		result = []
		code = self.escape
		for key, data in rows:
			value = key.split('#')[-1]
			if property.startswith('Beam'):
				value = code.S2Double(value)
			else:
				value = code.S2Int(value)
			result.append({
				'runID': runID,
				'rowkey': key,
				'property': property,
				'value': value,
				'count': int(data['data:count'])
				})
		return result

	def runsDetail(self, runIDs, property):
		runs = runIDs.replace(' ','').split(',')
		result = {}
		for run in runs:
			result[run] = self.runDetail(run, property)
		return result

	def query(self, command, load2file = True):
		start_time = datetime.now()
		com = command.replace(' ', '')
		run_list = com.split(':')[0].split(',')
		com = com.split(':')[1]
		com = '|' + com.replace("&&", "$&").replace("||", "$|")
		com_list = com.split('$')
		code = self.escape
		intmax = str(2147483647)
		intmin = str(-2147483647)
		doublemax = str(2147483647.0)
		doublemin = str(-2147483647.0)
		#re
		re_range = re.compile(r'(?P<logic>[\|\&])(?P<lower>-?\d*\.?\d+)(?P<math1>[\<\>\=]+)(?P<property>\w*)(?P<math2>[\<\>\=]+)(?P<upper>-?\d*\.?\d+)')
		re_bound = re.compile(r'(?P<logic>[\|\&])(?P<property>\w*)(?P<math>[\<\>\=]+)(?P<bound>-?\d*\.?\d+)')
		#query result
		queryobj = {}
		totalEvent = 0
		for run_no in run_list:
			#current run
			runquery = {}
			for query_str in com_list:
				this_query = {}
				logic = ''
				lower = ''
				upper = ''
				property = ''
				if re.match(re_range, query_str):
					range = re.match(re_range, query_str).groupdict()
					logic = range['logic']
					property = range['property']
					if property.startswith('Beam'):
						lower = range['lower']
						upper = range['upper']
					else:
						if range['math1'] == '<':
							lower = str(int(range['lower']) + 1)
						else:
							lower = str(int(range['lower']))
						if range['math2'] == '<':
							upper = str(int(range['upper']))
						else:
							upper = str(int(range['upper']) + 1)
				else:
					bound = re.match(re_bound, query_str).groupdict()
					logic = bound['logic']
					property = bound['property']
					if property.startswith('Beam'):
						if bound['math'].startswith('<'):
							lower = doublemin
							upper = bound['bound']
						else:
							lower = bound['bound']
							upper = doublemax
					else:
						if bound['math'] == '<':
							lower = intmin
							upper = str(int(bound['bound']))
						if bound['math'] == '<=':
							lower = intmin
							upper = str(int(bound['bound']) + 1)
						if bound['math'] == '>':
							lower = str(int(bound['bound']) + 1)
							upper = intmax
						if bound['math'] == '>=':
							lower = str(int(bound['bound']))
							upper = intmax
						if bound['math'] == '=':
							lower = str(int(bound['bound']))
							upper = str(int(bound['bound']) + 1)
				this_query = self.__query__(run_no, property, upper, lower)
				tmpobj = {}
				dataarr = [data for key,data in this_query]
				tmpobjarr = get_query_jsonobj(dataarr, self.tableName, self.fs)
				for obj in tmpobjarr:
					concat_jsonobj(tmpobj, obj)
					for dst in obj:
						totalEvent += len(obj[dst])
				for key in tmpobj:
					tmpobj[key] = set(tmpobj[key])
				if logic=='|':
					or_query_jsonobj(runquery, tmpobj)
				else:
					runquery = and_query_jsonobj(runquery, tmpobj)
			or_query_jsonobj(queryobj, runquery)
		stop_time = datetime.now()
		time_cost = (stop_time - start_time).total_seconds()
		count = 0
		revertObj = {}
		for key in queryobj:
			queryobj[key] = list(queryobj[key])
			querylen = len(queryobj[key])
			revertObj[key] = querylen
			count += querylen
		savePath = ''
		if load2file:
			saveFileName = str(uuid.uuid1())
			f = open('/root/eventdb/gx/EventDBWeb/static/data/' + saveFileName + '.json', 'w');
			savePath = '/static/data/' + saveFileName + '.json'
			f.write(json.dumps(queryobj))
			f.close()
		tmpobj = {
				'result': revertObj,
				'count': count,
				'time_cost': time_cost,
				'save_path': savePath,
				'total_event': totalEvent
				}
		#return json.dumps(tmpobj)
		return tmpobj

				
	def __query__(self, run_no, property, upper, lower):
		row_start = run_no + '#' + property + '#' + self.Value2Binary(lower)
		row_stop = run_no + '#' + property + '#' + self.Value2Binary(upper)
		return self.eventdb.scan(row_start = row_start, row_stop = row_stop)

	def Value2Binary(self, value):
		code = self.escape
		if value.find('.') != -1:
			binary = code.Double2S(float(value))
		else:
			binary = code.Int2S(int(value))
		return binary


	def close(self):
		self.conn.close()

class escape:
	def __init__(self, libPath = '/root/eventdb/gx/eventdb/TypeSer.so'):
		c_lib=cdll.LoadLibrary(libPath)
		c_lib.DoubleS.restype = ctypes.c_char_p
		c_lib.DoubleS.argtypes=[ctypes.c_double]
		c_lib.IntS.argtypes=[ctypes.c_int]
		c_lib.IntS.restype = ctypes.c_char_p

		c_lib.SDouble.restype = ctypes.c_double
		c_lib.SDouble.argtypes = [ctypes.c_char_p]
		c_lib.SInt.restype = ctypes.c_int
		c_lib.SInt.argtypes = [ctypes.c_char_p]

		self.clib = c_lib

	def Int2S(self, d):
		return self.clib.IntS(d)

	def Double2S(self, d):
		return self.clib.DoubleS(d)

	def S2Int(self, s):
		return self.clib.SInt(s)

	def S2Double(self, s):
		return self.clib.SDouble(s)

def get_query_jsonobj(dataarr, tableName, eventdb_fs):
	objarr = []
	if len(dataarr) == 0:
		return objarr
	run = dataarr[0]['data:run']
	datafile = '/eventdb/' + tableName + '/data/' + run + '.data'

	with eventdb_fs.open_file(datafile) as f:
		for data in dataarr:
			offset = int(data['data:offset'])
			length = int(data['data:length'])
			f.seek(offset)
			obj = json.loads(f.read(length))
			objarr.append(obj)
	
	return objarr

def concat_jsonobj(queryobj, obj):
	for key in obj:
		if key in queryobj:
			queryobj[key] = queryobj[key] + obj[key]
		else:
			queryobj[key] = obj[key]

def and_query_jsonobj(queryobj, tmpobj):
	tmpquery = {}
	for key in tmpobj:
		if key in queryobj:
			tmparr = queryobj[key] & tmpobj[key]
			if len(tmparr) != 0:
				tmpquery[key] = tmparr
	return tmpquery
			
def or_query_jsonobj(queryobj, tmpobj):
	for key in tmpobj:
		if key in queryobj:
			queryobj[key] = queryobj[key] | tmpobj[key]
		else:
			queryobj[key] = tmpobj[key]
