#!/bin/bash

#create talbe
./create_table $3

#analyze
./dispatch $1
mkdir $2
mkdir $2/csv
mkdir $2/data
./run2data $1 $2

#insert
hadoop fs -copyFromLocal $2 /eventdb/$3

#bulkload
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator="," -Dimporttsv.columns="HBASE_ROW_KEY,data:run,data:offset,data:length,data:count" HEP:$3 hdfs:///eventdb/$3/csv
