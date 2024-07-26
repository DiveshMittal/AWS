import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.sql.Date;
import java.util.ArrayList;



 /**
  * Performs SQL DDL and SELECT queries on a MySQL database hosted on AWS RDS.
  */
 public class AmazonRedshift 
 {
	private Connection con;
	private String url = "jdbc:redshift://default-workgroup.891377286653.eu-north-1.redshift-serverless.amazonaws.com:5439/dev";
	private String uid = "admin";
	private String pw = "Admin123";
     
    public static void main(String[] args) throws SQLException 
	{
		AmazonRedshift q = new AmazonRedshift();
        q.connect();
        //q.drop();
        //q.create();
        //q.insert();
		
		//Query1 ResultSettoString
		System.out.println("ResultSettoString \n");
		ResultSet rs1 = q.query1();
		System.out.println("Query 1 Results:");
		System.out.println(resultSetToString(rs1, 100)); 
		System.out.println("\n");
		
		//Query1 ResultSetMetaDataToString
		System.out.println("ResultSetMetaDataToString \n");
		ResultSetMetaData meta1 = rs1.getMetaData();
		System.out.println("Query 1 Metadata:");
		System.out.println(resultSetMetaDataToString(meta1));
		
		
		//Query2 ResultSettoString
		System.out.println("ResultSettoString \n");
		ResultSet rs2 = q.query2();
		System.out.println("Query 2 Results:");
		System.out.println(resultSetToString(rs2, 100)); 
		System.out.println("\n");
		
		//Query2 ResultSetMetaDataToString
		System.out.println("ResultSetMetaDataToString \n");
		ResultSetMetaData meta2 = rs2.getMetaData();
		System.out.println("Query 2 Metadata:");
		System.out.println(resultSetMetaDataToString(meta2));
		
		
		//Query3 ResultSettoString
		System.out.println("ResultSettoString \n");
		ResultSet rs3 = q.query3();
		System.out.println("Query 3 Results:");
		System.out.println(resultSetToString(rs3, 100)); 
		System.out.println("\n");
		
		//Query3 ResultSetMetaDataToString
		System.out.println("ResultSetMetaDataToString \n");
		ResultSetMetaData meta3 = rs3.getMetaData();
		System.out.println("Query 3 Metadata:");
		System.out.println(resultSetMetaDataToString(meta3));
		
        q.close();
     }
	 
    public Connection connect() throws SQLException 
	{
        // TODO: For connect to work you must configure your AWS connection info in the private instance variables at the top of the file.
        // If connection fails, make sure to modify your VPC rules to along inbound traffic to the database from your IP.
		
        System.out.println("Connecting to database.");
		con=DriverManager.getConnection(url, uid, pw);
		System.out.println("Connected...");
		return con;
        
		// Note: Must assign connection to instance variable as well as returning it back tothe caller
    }
		
	public void close() throws SQLException 
	{
        System.out.println("Closing database connection.");
		if (con != null) 
		{
			con.close();
		}
    }
	
	
    public void drop() throws SQLException 
	{		
        System.out.println("Dropping all the tables");
		Statement stmt=con.createStatement();
		stmt.execute("DROP schema dev CASCADE");
		stmt.close();
    }
	
    public void create() throws SQLException 
	{
        System.out.println("Creating Tables");
		Statement stmt=con.createStatement();
		
		// Read the tpch_create.sql file
		String filePath = "data/tpch_create.sql";

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) 
		{
            String line;
			StringBuilder sqlBuilder = new StringBuilder();
            while ((line = br.readLine()) != null) 
			{
                // Process each line
				sqlBuilder.append(line);
				sqlBuilder.append("\n");
            }
			String sql = sqlBuilder.toString();
			stmt.execute(sql);
        } 
		catch (IOException e) 
		{
            e.printStackTrace();
        }
		System.out.println("Creating Tables");
		stmt.close();
		
    }
	
    public void insert() throws SQLException 
	{
        System.out.println("Loading TPC-H Data");
		String directoryPath = "data/";
		Statement stmt=con.createStatement();

		File folder = new File(directoryPath);
        File[] listOfFiles = folder.listFiles();
        List<String> filePaths = new ArrayList<>();
		
		if (listOfFiles != null) 
		{
            for (File file : listOfFiles) 
			{
                if (file.isFile() && file.getName().endsWith(".sql") && !file.getName().endsWith("create.sql")) 
				{
                    filePaths.add(file.getAbsolutePath());
                }
            }
        } 
		else 
		{
            System.out.println("The specified directory does not exist or is not a directory.");
            return;
        }
		
		for (String filePath : filePaths) 
		{
			
			try (BufferedReader br = new BufferedReader(new FileReader(filePath))) 
			{
				String line;
				String sql = new String(Files.readAllBytes(Paths.get(filePath)));
				stmt.execute(sql);
				
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
			System.out.println("Data Inserted for" + filePath);	
		}
		stmt.close();
    }
	
		
        
	
    public ResultSet query1() throws SQLException 
	{
        System.out.println("Executing query #1.");
		String query1= "select o_custKey, c.c_name, r.r_name ,o_orderdate,sum(o_totalprice)  totalSales from orders o join customer c on c.c_custkey=o.o_custKey JOIN nation n ON c.c_nationkey=n.n_nationkey join region r on n.n_regionkey = r.r_regionkey where r.r_name='AMERICA' group by o_orderdate,o_custKey,c.c_name, r.r_name order by o_orderdate desc limit 10";
		
		Statement stmt=con.createStatement();
		
		return stmt.executeQuery(query1);
    }
	
    public ResultSet query2() throws SQLException 
	{
        System.out.println("Executing query #2.");
        String query2 = "with largesegment as ("
              + "select r.r_regionkey, count(c_custkey) as cnt "
              + "from customer C "
              + "JOIN nation n ON c.c_nationkey = n.n_nationkey "
              + "join region r on n.n_regionkey = r.r_regionkey "
              + "where r.r_name <> 'EUROPE' "
              + "group by r.r_regionkey "
              + "order by cnt desc limit 1) "
              + "select o_custKey, o_totalprice "
              + "from orders o "
              + "join customer c on c.c_custkey = o.o_custKey "
              + "JOIN nation n ON c.c_nationkey = n.n_nationkey "
              + "join largesegment r on n.n_regionkey = r.r_regionkey "
              + "order by o_totalprice desc;";
		
		Statement stmt=con.createStatement();
		
		return stmt.executeQuery(query2);
	}
	
    public ResultSet query3() throws SQLException {
        System.out.println("Executing query #3.");
		String query3 = "SELECT O.O_ORDERPRIORITY, COUNT(l_linenumber) "
             + "FROM ORDERS O "
             + "JOIN lineitem L ON L.l_orderkey = O.O_ORDERKEY "
             + "WHERE O_ORDERDATE BETWEEN '2017-04-01' AND '2023-04-01' "
             + "GROUP BY O.O_ORDERPRIORITY "
             + "ORDER BY O.O_ORDERPRIORITY ASC;";

        Statement stmt=con.createStatement();
		
		return stmt.executeQuery(query3);
    }
	
	
    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;
        ResultSetMetaData meta = rst.getMetaData();
        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');
        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j));
        buf.append('\n');
        while (rst.next()) {
            if (rowCount < maxrows) {
                for (int j = 0; j < meta.getColumnCount(); j++) {
                    Object obj = rst.getObject(j + 1);
                    buf.append(obj);
                    if (j != meta.getColumnCount() - 1)
                        buf.append(", ");
                }
                buf.append('\n');
            }
            rowCount++;
        }
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }
	
    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException
	{
		StringBuffer buf = new StringBuffer(5000);
		buf.append(meta.getColumnName(1)+" ("+meta.getColumnLabel(1)+", "+meta.getColumnType(1)+"-"+meta.getColumnTypeName(1)+", "+meta.getColumnDisplaySize(1)+", "+meta.getPrecision(1)+", "+meta.getScale(1)+ ")" );
		for (int j = 2; j <= meta.getColumnCount(); j++)
			buf.append(", " + meta.getColumnName(j) + " (" + meta.getColumnLabel(j)+", " + meta.getColumnType(j) + "-" + meta.getColumnTypeName(j) + ", " + meta.getColumnDisplaySize(j) + ", " + meta.getPrecision(j) + ", " + meta.getScale(j) +") ");
		return buf.toString();
	}
 }