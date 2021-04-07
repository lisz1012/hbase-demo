package com.lisz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class HBaseDemo {
	private Configuration conf = null;
	private Connection conn = null;
	private Admin admin;
	private TableName tableName = TableName.valueOf("phone");

	@Before
	public void init() throws IOException {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-02,hadoop-03,hadoop-04");
		conn = ConnectionFactory.createConnection(conf);
		admin = conn.getAdmin();
	}

	@Test
	public void createTable() throws Exception{
		if (!admin.tableExists(tableName)){
			TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
			ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes());
			//tableDescriptorBuilder.setColumnFamilies(Collections.singleton(columnFamilyDescriptorBuilder.build()));
			tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
			admin.createTable(tableDescriptorBuilder.build());
		}
	}

	@After
	public void destroy(){
		try {
			admin.close();
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
