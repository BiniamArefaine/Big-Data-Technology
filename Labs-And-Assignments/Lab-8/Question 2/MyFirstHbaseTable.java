package hbaseTest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable
{

	private static final String TABLE_NAME = "employees";
	private static final String CF_DEFAULT = "employees_Infos";

	public static void main(String... args) throws IOException
	{

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor("prof_Infos"));

			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName()))
			{
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
			
			System.out.print("Creating columns.... ");
			Table myTable = connection.getTable(table.getTableName());
			byte[] personal_CF = Bytes.toBytes(CF_DEFAULT);
		    byte[] professional_CF = Bytes.toBytes("prof_Infos");
		    byte[] column1 = Bytes.toBytes("name");
		    byte[] column2 = Bytes.toBytes("cty");
		    byte[] column3 = Bytes.toBytes("dsigntion");
		    byte[] column4 = Bytes.toBytes("slry");

		    
			System.out.print("Creating datas.... ");
			System.out.print("Creating RowKey-1.... ");
		    Put put;
			put = new Put(Bytes.toBytes("1"));
			put.addColumn(personal_CF, column1, Bytes.toBytes("John"));
			put.addColumn(personal_CF, column2, Bytes.toBytes("Boston"));
			put.addColumn(professional_CF, column3, Bytes.toBytes("Manager"));
			put.addColumn(professional_CF, column4, Bytes.toBytes("150000"));
			myTable.put(put);
			
			System.out.print("Creating RowKey-2.... ");
			put = new Put(Bytes.toBytes("2"));
			put.addColumn(personal_CF, column1, Bytes.toBytes("Mary"));
			put.addColumn(personal_CF, column2, Bytes.toBytes("New York"));
			put.addColumn(professional_CF, column3, Bytes.toBytes("Sr. Engineer"));
			put.addColumn(professional_CF, column4, Bytes.toBytes("130000"));
			myTable.put(put);
			
			System.out.print("Creating RowKey-3.... ");
			put = new Put(Bytes.toBytes("3"));
			put.addColumn(personal_CF, column1, Bytes.toBytes("Bob"));
			put.addColumn(personal_CF, column2, Bytes.toBytes("Fremont"));
			put.addColumn(professional_CF, column3, Bytes.toBytes("Jr. Engineer"));
			put.addColumn(professional_CF, column4, Bytes.toBytes("90000"));
			myTable.put(put);
			
					
			System.out.println("Updating  Bob....");	
			Get get = new Get(Bytes.toBytes("3"));
			put = new Put(Bytes.toBytes("3"));
			
			Result result = myTable.get(get);					
			
			System.out.println("Updating  Bob to Sr. Engineer position....");	
			byte [] value = result.getValue(professional_CF,column4);											
			put.addColumn(professional_CF, column3, Bytes.toBytes("Sr. Engineer"));

			
			System.out.println("5% increase in Bob's salary....");		
			Integer integerVal= (int) (Integer.parseInt(Bytes.toString(value)) * 1.05); 
			put.addColumn(professional_CF, column4, Bytes.toBytes(integerVal.toString()));
			myTable.put(put);


			System.out.println(" Successfully Done!");
		}
	}
}