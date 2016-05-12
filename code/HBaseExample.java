package hBaseExample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

public class HBaseExample {

	/* API can be found here:
		https://hbase.apache.org/apidocs/
	*/

	private static Configuration conf;
	private static Connection connection;
	private static Admin admin;
	
	// A buffer to tempory store for put request, used to speed up the process of putting records into hbase
	private static List<Put> putList;
	private static int listCapacity = 1000000;
	
	// TODO: Set up your tableName and columnFamilies in the table
	private static String[] tableName = {"s104062702:eng", "s104062702:math"};
	private static String[] tableColFamilies = {"grade"};

	public static void createTable(String tableName, String[] colFamilies) throws Exception {
		// Instantiating table descriptor class
		TableName hTableName = TableName.valueOf(tableName);
		if (admin.tableExists(hTableName)) {
			System.out.println(tableName + " : Table already exists!");
		} else {
			HTableDescriptor tableDescriptor = new HTableDescriptor(hTableName);
			// TODO: Adding column families to table descriptor
			for (String cf : colFamilies) {
				tableDescriptor.addFamily(new HColumnDescriptor(cf));
			}
			// TODO: Admin creates table by HTableDescriptor instance
			System.out.println("Creating table: " + tableName + "...");
			admin.createTable(tableDescriptor);
			System.out.println("Table created");
		}
	}
	
	public static void removeTable(String tableName) throws Exception {
		TableName hTableName = TableName.valueOf(tableName);
		if (!admin.tableExists(hTableName)) {
			System.out.println(tableName + ": Table does not exist!");
		} else {
			System.out.println("Deleting table: " + tableName + "...");
			// TODO: disable & drop table
			if (!admin.isTableDisabled(hTableName)) {
				admin.disableTable(hTableName);
				admin.deleteTable(hTableName);
			}
			System.out.println("Table deleted");
		}
	}
	
	public static void addRecordToPutList(String rowKey, String colFamily,
			String qualifier, String value) throws Exception {
		// TODO: use Put to wrap information and put it to PutList.
		//System.out.println("addRecordToPutList " + rowKey + ", " + colFamily + ", " + qualifier + ", " + value);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(
			Bytes.toBytes(colFamily),
			Bytes.toBytes(qualifier),
			Bytes.toBytes(value));
		putList.add(put);
	}
	
	public static void addRecordToHBase(String tableName) throws Exception {
		// TODO: dump things from memory (PutList) to HBaseConfiguration
		Table table = connection.getTable(TableName.valueOf(tableName));
		for (Put p : putList) {
			table.put(p);
		}
		putList.clear();
	}
	
	public static void deleteRecord(String tableName, String rowKey) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tableName));
		// TODO use Delete to wrap key information and use Table api to delete it.
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
	}
	
	public static void main(String[] args){
		
		try {
			// Instantiating hbase connection
			conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
			for (int curTableID = 0; curTableID < args.length; ++curTableID) {
				System.out.println("fetching " + args[curTableID] + "...");
				// remove the old table on hbase
				removeTable(tableName[curTableID]);
				// create a new table on hbase
				createTable(tableName[curTableID], tableColFamilies);
				// Read Content from local file
				File file = new File(args[curTableID]);
				BufferedReader br = new BufferedReader(new FileReader(file));
				int linecount = 0;
				String line = null;
				String idf;
				if (curTableID == 0)
					idf = "english";
				else
					idf = "math";

				putList = new ArrayList<Put>(listCapacity);
				while (null != (line = br.readLine())) {
					if (0 == linecount % 100000) {
						System.out.println("INFO: " + linecount + " lines added to hbase.");
					}
					if (0 == linecount % 1000) {
						System.out.println("INFO: " + linecount);
					}
					// System.out.println(line);
					// TODO : Split the content of a line and store it to hbase
					String[] rk = line.split("\\s+", 2);
					addRecordToPutList(
						rk[0],
						"grade", // CF
						"name",
						rk[0]
					);
					if (putList.size() == listCapacity) {
						addRecordToHBase(tableName[curTableID]);
					}
					addRecordToPutList(
						rk[0],
						"grade", // CF
						idf,
						rk[1]
					);
					
					// TODO : Add the record to corresponding hbase table
					
					// if capacity of our putList buffer is reached, dump them into HBase
					if (putList.size() == listCapacity) {
						addRecordToHBase(tableName[curTableID]);
					}
					++linecount;
				}
				System.out.println(linecount + " lines added to hbase.");
				// dump remaining contents into HBase
				addRecordToHBase(tableName[curTableID]);
				br.close();
			}
			// Finalize and close connection to Hbase
			admin.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
