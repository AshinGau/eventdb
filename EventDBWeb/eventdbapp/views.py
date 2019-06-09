import happybase
from django.shortcuts import render
import json
from django.http import JsonResponse
from eventdb import events,db

# Create your views here.

def runInfo(request):
	op = request.GET['op']
	conn = happybase.Connection('192.168.60.64')
	table = conn.table('runInfo')
	result = 0
	if op == 'totalEvents':
		result = table.counter_get('totalEvents', 'data:value')
	if op == 'volume':
		result = str(table.counter_get('totalEvents', 'data:value') / 1024) + 'GB'
	conn.close()
	return JsonResponse(result, safe=False)

def index(request):
	tablename=request.GET['tablename']
	command = request.GET['command']
	info=db.runInfo(tablename)
	print(command)
	result = info.query(command)
	return JsonResponse(result, safe=False)

def detail(request):
	tablename=request.GET['tablename']
	runs=request.GET['runs']
	property=request.GET['property']
	info=db.runInfo(str(tablename))
	runs=str(runs)
	property=str(property)
	print runs,property 
	result=info.runsDetail(runs,property)
	return JsonResponse(result,safe=False)

def show(request):
	tablename=request.GET['tablename']
	pageindex=int(request.GET['pageindex'])
	rows=int(request.GET['rows'])
	info=db.runInfo(str(tablename))
	result=info.page(pageIndex=pageindex,rows=rows)
	return JsonResponse(result,safe=False)

def indexpage(request):
	return render(request, 'index.html',locals())

def query(request):
	return render(request,'query.html',locals())

def queryHistory(request):
	return render(request,'queryHistory.html',locals())

def rundetail(request):
	return render(request,'rundetail.html',locals())

def viewrun(request):
	return render(request,'viewrun.html',locals())

def viewfits(request):
	return render(request,'viewfits.html',locals())

def viewrootOperator(request):
	return render(request,'rootOperator.html',locals())
