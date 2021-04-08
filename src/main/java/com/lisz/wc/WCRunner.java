package com.lisz.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class WCRunner {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		conf.set("hbase.zookeeper.quorum", "hadoop-02,hadoop-03,hadoop-04");
		conf.set("mapreduce.app-submission.cross-platform","true");
		conf.set("mapreduce.framework.name","local");
		Job job = Job.getInstance(conf);

		job.setJarByClass(WCRunner.class);
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//TableMapReduceUtil.initTableMapperJob();
		// Table "wc" must be created first
		TableMapReduceUtil.initTableReducerJob("wc", WCReducer.class, job, null, null, null, null, false); // set to true when not running locally
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Put.class);
		FileInputFormat.addInputPath(job, new Path("/wc/wc"));


		job.waitForCompletion(true);
	}
}
