package com.lisz.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable ONE = new IntWritable(1);
	private static final Text WORD = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()){
			String s = itr.nextToken();
			WORD.set(s);
			context.write(WORD, ONE);
		}
	}
}
