/**
*Title: load csv-file into hbase-table
*Description: load csv-file into hbase-table
*Copyright: Copyright (c) 2017
*Organization: XJTU-OSV
*Author Ashin E-mail: ashingau@outlook.com
*/
package org.osv.eventdb;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;   
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;  
import org.apache.hadoop.hbase.mapreduce.TableReducer;  
import org.apache.hadoop.hbase.util.Bytes;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 


/**
 * load csv-file into hbase-tabel
 */
public class Csv2hbase {
	public static class CsvMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fl = (FileSplit) context.getInputSplit();
			String csvName = fl.getPath().getName();
			String dstName = csvName.substring(0, csvName.length() - 4);			
			String[] vs = value.toString().split(",");
			String run = dstName.split("_")[2].substring(3);
			String pp = vs[0];
			String vl = vs[1];
			String et = vs[2];
			Text rowkey = new Text("-" + run + "#" + pp + "#" + vl);
			Text fileIndex = new Text(dstName + "#" + et);
			context.write(rowkey, fileIndex);
		}
	}

	public static class CsvReducer extends TableReducer<Text, Text, NullWritable> {
		@Override 
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, ArrayList<Integer>> mp = new HashMap<String, ArrayList<Integer>>();	
			Integer sum = 0;
			for (Text val : values) {
				String[] sp = val.toString().split("#");
				String dn = sp[0];
				String et = sp[1];
				if (mp.containsKey(dn)) {
					mp.get(dn).add(new Integer(et));
				} else {
					ArrayList<Integer> lst = new ArrayList<Integer>();
					lst.add(new Integer(et));
					mp.put(dn, lst);
				}
				sum++;
			}
		
			StringBuffer sb = new StringBuffer();
			sb.append("{");
			for (String mpk : mp.keySet()) {
				ArrayList<Integer> lst = mp.get(mpk);
				sb.append(mpk);
				sb.append(":");
				sb.append(lst.toString().replaceAll(" ", ""));
				sb.append(",");			
			}
			sb.setCharAt(sb.length() - 1, '}');	
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("FileIndex"), Bytes.toBytes(sb.toString()));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("count"), Bytes.toBytes(sum.toString()));
			
			context.write(NullWritable.get(), put);
		}

	}

	public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String tableName = args[1];

        Configuration conf = new Configuration();
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        Job job = Job.getInstance(conf, "csv2hbase");
        job.setJarByClass(Csv2hbase.class);
        job.setMapperClass(CsvMapper.class);
        job.setReducerClass(CsvReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        
        job.setNumReduceTasks(3);
        
        System.exit(job.waitForCompletion(true)? 0: 1);
	}

}
