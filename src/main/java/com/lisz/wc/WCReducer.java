package com.lisz.wc;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/*
Word Count
1. Read HDFS data
2. Ssave the data to HBase
 */
public class WCReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i : values) {
			sum += i.get();
		}
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ct"), Bytes.toBytes(sum + ""));

		context.write(null, put);
	}

}
