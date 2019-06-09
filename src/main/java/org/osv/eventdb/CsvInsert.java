/**
*Title: load csv-file into hbase-table
*Description: load csv-file into hbase-table
*Copyright: Copyright (c) 2017
*Organization: XJTU-OSV
*Author Ashin E-mail: ashingau@outlook.com
*/
package org.osv.eventdb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.hadoop.hbase.client.Put;


/**
 * load csv-file into hbase-tabel
 */
public class CsvInsert {
	static Logger logger = LoggerFactory.getLogger(Csv2hbase.class);
	public static class CsvMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] vs = value.toString().split(",");
			String hkey = vs[0];
			String run = vs[1];
			String offset = vs[2];
			String length = vs[3];
			String count = vs[4];

			final byte[] rowKey = Bytes.toBytes(hkey);
            final ImmutableBytesWritable HKey = new ImmutableBytesWritable(rowKey);
			Put put = new Put(rowKey);
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("run"), Bytes.toBytes(run));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("offset"), Bytes.toBytes(offset));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("length"), Bytes.toBytes(length));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("count"), Bytes.toBytes(count));
			
			context.write(HKey, put);
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
        String inputPath = args[0];
        String outputPath = args[1];
        String tableName = args[2];
        
        Job job = Job.getInstance(conf, "CsvInsert");
        job.setJarByClass(Csv2hbase.class);
        job.setMapperClass(CsvMapper.class);  
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
        job.setMapOutputValueClass(Put.class);  
        // speculation  
        job.setSpeculativeExecution(false);  
        job.setReduceSpeculativeExecution(false);  
        // in/out format  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(HFileOutputFormat2.class);  

        FileInputFormat.setInputPaths(job, inputPath);  
        FileOutputFormat.setOutputPath(job, new Path(outputPath));  

        Connection connection = ConnectionFactory.createConnection(conf);
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        
        Admin admin = connection.getAdmin();
        HFileOutputFormat2.configureIncrementalLoad(job, hTable, connection.getRegionLocator(TableName.valueOf(tableName)));
        if (job.waitForCompletion(true)) {  
        	LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
            load.doBulkLoad(new Path(outputPath), admin, hTable,connection.getRegionLocator(TableName.valueOf(tableName)));
            System.exit(0);
        } else {  
            logger.error("loading failed.");  
            System.exit(1);  
        }
	}

}
