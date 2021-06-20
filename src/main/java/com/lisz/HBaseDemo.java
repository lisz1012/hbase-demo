package com.lisz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HBaseDemo {
	private Configuration conf = null;
	private Connection conn = null;
	private Admin admin;
	private TableName tableName = TableName.valueOf("phone");
	private Table table;
	private Random rand = new Random();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	@Before
	public void init() throws IOException {
		// We can use factory pattern to create table object with above configs
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-02,hadoop-03,hadoop-04");
		conn = ConnectionFactory.createConnection(conf);
		// Table level operations
		admin = conn.getAdmin();
		// Data level operation
		table = conn.getTable(tableName);
	}

	@Test
	public void createTable() throws Exception{
		if (admin.tableExists(tableName)){
			// Disable table first before delete it, like in the cli.
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
		ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes());
		//tableDescriptorBuilder.setColumnFamilies(Collections.singleton(columnFamilyDescriptorBuilder.build()));
		tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
		admin.createTable(tableDescriptorBuilder.build());
	}

	@Test
	public void insert() throws Exception{
		Put put = new Put(Bytes.toBytes("2222"));
		put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("John"));
		put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(12));
		put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
		table.put(put);
		// admin.flush(tableName);
		/*
			hbase(main):014:0> scan 'phone'
			ROW                                    COLUMN+CELL
            1111                                  column=cf:age, timestamp=2021-04-07T15:32:14.070, value=\x00\x00\x00\x0C
            1111                                  column=cf:gender, timestamp=2021-04-07T15:32:14.070, value=male
            1111                                  column=cf:name, timestamp=2021-04-07T15:32:14.070, value=John
			1 row(s)
			Took 0.1584 seconds
		 */
	}

	@Test
	public void get() throws Exception{
		Get get = new Get(Bytes.toBytes("2222"));
		// By default user can get all colums, but in this example, only get the column family: "cf:name", "cf:age", "cf:name",
		// will throw Exception when trying to get "cf:other", since Hbase could have millions of columns,
		// this is to avoid unnecessary IO
		get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
		get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"));
		get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("gender"));
		Result result = table.get(get);
		Cell cell1 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("name"));
		Cell cell2 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("age"));
		Cell cell3 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("gender"));

		byte[] bytes = CellUtil.cloneValue(cell1);
		String name = Bytes.toString(bytes);
		bytes = CellUtil.cloneValue(cell2);
		int age = Bytes.toInt(bytes);
		bytes = CellUtil.cloneValue(cell3);
		String gender = Bytes.toString(bytes);

		System.out.println("name: " + name + " age: " + age + " gender: " + gender) ;
	}

	@Test
	public void scan() throws Exception{
		Scan scan = new Scan();
		// scan.setCaching(1000);
		// Specify the range of row key, which is sorted in alphabet order.
//		scan.withStartRow();
//		scan.withStopRow();
		// NOT suggested to scan the whole table. But HBase can put the data queried by single, multiple and really hot data (like the permanent space in JVM or LRU)
		// to different area.
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		while (iterator.hasNext()){
			Result next = iterator.next();
			Cell cell1 = next.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("name"));
			Cell cell2 = next.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("age"));
			Cell cell3 = next.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("gender"));
			String name = Bytes.toString(CellUtil.cloneValue(cell1));
			int age = Bytes.toInt(CellUtil.cloneValue(cell2));
			String gender = Bytes.toString(CellUtil.cloneValue(cell3));

			System.out.println("name: " + name + " age: " + age + " gender: " + gender) ;
		}
		scanner.close();
	}

	@Test
	public void insertCallingData() throws Exception{
		List<Put> list = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			String phoneNumber = getNumber("+1");
			for (int j = 0; j < 10000; j++) {
				String fromNumber = getNumber("+1");
				String length = String.valueOf(rand.nextInt(100));
				String date = getDate("2021");
				String type = String.valueOf(rand.nextInt(2));

				String rowKey = fromNumber + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());
				Put put = new Put(Bytes.toBytes(rowKey));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fromNumber"), Bytes.toBytes(fromNumber));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("length"), Bytes.toBytes(length));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("date"), Bytes.toBytes(date));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes(type));
				list.add(put);
			}
		}
		table.put(list);
	}

	private String getDate(String s) {
		return s + String.format("%02d%02d%02d%02d%02d",
				rand.nextInt(12) + 1, rand.nextInt(31), rand.nextInt(24),
				rand.nextInt(60), rand.nextInt(60));
	}

	private String getNumber(String str) {
		return str + String.format("%10s", rand.nextInt(99999) + "" + rand.nextInt(99999));
	}

	/*
	  Look up the user's calling records in March
    */
	@Test
	public void scanByCondition() throws Exception{
		String startRow = "+19999760910" + "_" + (Long.MAX_VALUE - sdf.parse("20211031235959").getTime());
		String stopRow = "+19999760910" + "_" + (Long.MAX_VALUE - sdf.parse("20210301000000").getTime());
		Scan scan = new Scan();
		scan.withStartRow(Bytes.toBytes(startRow));
		scan.withStopRow(Bytes.toBytes(stopRow));

		System.out.println(startRow);
		System.out.println(stopRow);

		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			String rowKey = new String(result.getRow());

			Cell cell = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("fromNumber"));
			String fromNumber = Bytes.toString(CellUtil.cloneValue(cell));

			cell = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("length"));
			// Note：Source Code Bug here for Bytes.toInt
			// Error message： offset (0) + length (4) exceed the capacity of the array: 2
			// for input "40" (byte array length = 2)
			// So better to use Bytes.toString and then parse it
			int length = Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell)));

			cell = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("date"));
			String date = Bytes.toString(CellUtil.cloneValue(cell));

			cell = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("type"));
			String type = Bytes.toString(CellUtil.cloneValue(cell));

			System.out.println(rowKey + "   " + fromNumber + "  " + length + "  " + date + "  " + type);
		}
	}

	/*
	Look up the calls from the current number (type = "0"):
	https://hbase.apache.org/2.3/book.html#client.filter
	 */
	@Test
	public void getType() throws Exception{
		Scan scan = new Scan();
		FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

		Filter filter1 = new PrefixFilter(Bytes.toBytes("+19999760910")); // +1999
		// We can also use another SingleColumnValueFilter as there's a column called "fromNumber"
//		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
//			Bytes.toBytes("cf"),
//			Bytes.toBytes("fromNumber"),
//			CompareOperator.EQUAL,
//			Bytes.toBytes("+19999760910")
//		);
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes("cf"),
				Bytes.toBytes("type"),
				CompareOperator.EQUAL,
				Bytes.toBytes("0")
		);
		filters.addFilter(filter1);
		filters.addFilter(filter2);
		scan.setFilter(filters);
		ResultScanner scanner = table.getScanner(scan);
		for (Result res : scanner) {
			String rowKey = new String(res.getRow());

			Cell cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("fromNumber"));
			String fromNumber = Bytes.toString(CellUtil.cloneValue(cell));

			cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("length"));
			// Note：Source Code Bug here for Bytes.toInt
			// Error message： offset (0) + length (4) exceed the capacity of the array: 2
			// for input "40" (byte array length = 2)
			// So better to use Bytes.toString and then parse it
			int length = Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell)));

			cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("date"));
			String date = Bytes.toString(CellUtil.cloneValue(cell));

			cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("type"));
			String type = Bytes.toString(CellUtil.cloneValue(cell));

			System.out.println(rowKey + "   " + fromNumber + "  " + length + "  " + date + "  " + type);
		}
	}

	/*
	Save the data with ProtoBuf, by doing this, we can save ~70% of the space, but can't query with conditions any more
	 */
	@Test
	public void insertWithProtoBuf() throws Exception{
		List<Put> list = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			String phoneNumber = getNumber("+1");
			for (int j = 0; j < 10000; j++) {
				String fromNumber = getNumber("+1");
				String length = String.valueOf(rand.nextInt(100));
				String date = getDate("2021");
				String type = String.valueOf(rand.nextInt(2));

				Phone.PhoneDetail.Builder builder = Phone.PhoneDetail.newBuilder();
				builder.setFromNumber(fromNumber).setLength(length).setDate(date).setType(type);
				Phone.PhoneDetail phoneDetail = builder.build();

				String rowKey = fromNumber + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());
				Put put = new Put(Bytes.toBytes(rowKey));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("phone"), phoneDetail.toByteArray());

				list.add(put);
			}
		}
		table.put(list);
	}

	@Test
	public void getByProtoBuf() throws Exception{
		String rowKey = "+19999864246_9223370426672396807";
		Get get = new Get(Bytes.toBytes(rowKey));
		//get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("phone"));  <-- This is optional
		Result result = table.get(get);

		String rk = Bytes.toString(result.getRow());
		byte[] bytes = CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("phone")));
		Phone.PhoneDetail phoneDetail = Phone.PhoneDetail.parseFrom(bytes);
		System.out.println(phoneDetail);
	}

	@Test
	public void delete() throws Exception{
		Delete delete = new Delete(Bytes.toBytes("+19893015432_9223370401271234807"));
		table.delete(delete);
	}

	@After
	public void destroy(){
		try {
			table.close(); // 原先有落地缓冲区数据的flushCommits，现在的2.3.5好像没有这个了
			admin.close(); // 空的
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
