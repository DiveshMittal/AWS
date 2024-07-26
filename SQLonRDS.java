import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.*;

public class SQLonRDS 
{
	private Connection con;
	
	private String url = "iitjdb.cv80wcamu8lz.eu-north-1.rds.amazonaws.com:3306";
	private String uid = "admin";
	private String pw = "G23AI1010";
	
	public static void main(String[] args) 
	{
		SQLonRDS q = new SQLonRDS();
		try 
		{
			q.connect();
			System.out.println("\n");
			q.drop();
			System.out.println("\n");
			q.create();
			System.out.println("\n");
			q.insert();
			System.out.println("\n");
			q.delete();
			System.out.println("\n");
			
			//Query1 ResultSettoString
			System.out.println("ResultSettoString \n");
			
			ResultSet rs1 = q.queryOne();

			System.out.println("Query 1 Results:");
			System.out.println(resultSetToString(rs1, 100)); 
			
			System.out.println("\n");
			
			//Query2 ResultSettoString
			ResultSet rs2 = q.queryTwo();

			System.out.println("Query 2 Results:");
			System.out.println(resultSetToString(rs2, 100)); 
			
			System.out.println("\n");
			
			//Query3 ResultSettoString
			ResultSet rs3 = q.queryThree();
			System.out.println("Query 3 Results:");
			System.out.println(resultSetToString(rs3, 100)); 

			System.out.println("\n");
			
			
			System.out.println("ResultSetMetaDataToString \n");
			//Query1 ResultSetMetaDataToString
			ResultSetMetaData meta1 = rs1.getMetaData();

			System.out.println("Query 1 Metadata:");
			System.out.println(resultSetMetaDataToString(meta1));

			System.out.println("\n");

			//Query2 ResultSetMetaDataToString
			ResultSetMetaData meta2 = rs2.getMetaData();

			System.out.println("Query 2 Metadata:");
			System.out.println(resultSetMetaDataToString(meta2));

			System.out.println("\n");
			
			//Query3 ResultSetMetaDataToString
			ResultSetMetaData meta3 = rs3.getMetaData();

			System.out.println("Query 3 Metadata:");
			System.out.println(resultSetMetaDataToString(meta3));


			q.close();
		} 
		catch (SQLException | ClassNotFoundException e) 
		{
			e.printStackTrace();
			System.out.println("failed");
		}
	}
	public void connect() throws SQLException, ClassNotFoundException 
	{
		// Load MySQL JDBC Driver
		Class.forName("com.mysql.cj.jdbc.Driver");
		String jdbcUrl = "jdbc:mysql://" + url + "/mydb?user=" + uid + "&password=" + pw;
		System.out.println("Connecting to database");
		con = DriverManager.getConnection(jdbcUrl);
		System.out.println("Connection Successful");
	}
	
	public void create() throws SQLException 
	{
		//TODO for create table
		String create_companyTable="CREATE TABLE company ( id INT PRIMARY KEY, name VARCHAR(50), ticker CHAR(10), annualRevenue DECIMAL(15, 2), numEmployees INT)";
		String create_SPTable="CREATE TABLE stockprice (companyId INT,priceDate DATE,openPrice DECIMAL(8, 2),highPrice DECIMAL(8, 2),lowPrice DECIMAL(8, 2),closePrice DECIMAL(8, 2),volume INT,PRIMARY KEY (companyId, priceDate),FOREIGN KEY (companyId) REFERENCES company(id))";
		
		//create statement execution
		Statement stmt=con.createStatement();
		
		stmt.executeUpdate(create_companyTable);
		System.out.println("Company Table Created.");
		
		stmt.executeUpdate(create_SPTable);
		System.out.println("Stock Price Table Created.");
	
	}
	public void drop() throws SQLException 
	{
		//TODO for drop table
		
		String drop_companyTable="DROP TABLE IF EXISTS company";
		String drop_SPTable="DROP TABLE IF EXISTS stockprice";
		
		//create statement execution
		Statement stmt=con.createStatement();
		
		stmt.executeUpdate(drop_SPTable);
		System.out.println("Stock Price Table Dropped.");
		
		stmt.executeUpdate(drop_companyTable);
		System.out.println("Company Table Dropped.");
	}
	public void insert() throws SQLException 
	{
		//TODO for insert query
		String Insert_company="INSERT INTO company (id, name, ticker, annualRevenue, numEmployees) VALUES (1, 'Apple', 'AAPL', 387540000000.00 , 154000),(2, 'GameStop', 'GME', 611000000.00, 12000),(3, 'Handy Repair', null, 2000000, 50),(4, 'Microsoft', 'MSFT', '198270000000.00' , 221000),(5, 'StartUp', null, 50000, 3)";
		String Insert_SP="INSERT INTO stockprice (companyId, priceDate, openPrice, highPrice, lowPrice, closePrice, volume) VALUES (1, '2022-08-15', 171.52, 173.39, 171.35, 173.19, 54091700),(1, '2022-08-16', 172.78, 173.71, 171.66, 173.03, 56377100),(1, '2022-08-17', 172.77, 176.15, 172.57, 174.55, 79542000),(1, '2022-08-18', 173.75, 174.90, 173.12, 174.15, 62290100),(1, '2022-08-19', 173.03, 173.74, 171.31, 171.52, 70211500),(1, '2022-08-22', 169.69, 169.86, 167.14, 167.57, 69026800),(1, '2022-08-23', 167.08, 168.71, 166.65, 167.23, 54147100),(1, '2022-08-24', 167.32, 168.11, 166.25, 167.53, 53841500),(1, '2022-08-25', 168.78, 170.14, 168.35, 170.03, 51218200),(1, '2022-08-26', 170.57, 171.05, 163.56, 163.62, 78823500),(1, '2022-08-29', 161.15, 162.90, 159.82, 161.38, 73314000),(1, '2022-08-30', 162.13, 162.56, 157.72, 158.91, 77906200),(2, '2022-08-15', 39.75, 40.39, 38.81, 39.68, 5243100),(2, '2022-08-16', 39.17, 45.53, 38.60, 42.19, 23602800),(2, '2022-08-17', 42.18, 44.36, 40.41, 40.52, 9766400),(2, '2022-08-18', 39.27, 40.07, 37.34, 37.93, 8145400),(2, '2022-08-19', 35.18, 37.19, 34.67, 36.49, 9525600),(2, '2022-08-22', 34.31, 36.20, 34.20, 34.50, 5798600),(2, '2022-08-23', 34.70, 34.99, 33.45, 33.53, 4836300),(2, '2022-08-24', 34.00, 34.94, 32.44, 32.50, 5620300),(2, '2022-08-25', 32.84, 32.89, 31.50, 31.96, 4726300),(2, '2022-08-26', 31.50, 32.38, 30.63, 30.94, 4289500),(2, '2022-08-29', 30.48, 32.75, 30.38, 31.55, 4292700),(2, '2022-08-30', 31.62, 31.87, 29.42, 29.84, 5060200),(4, '2022-08-15', 291.00, 294.18, 290.11, 293.47, 18085700),(4, '2022-08-16', 291.99, 294.04, 290.42, 292.71, 18102900),(4, '2022-08-17', 289.74, 293.35, 289.47, 291.32, 18253400),(4, '2022-08-18', 290.19, 291.91, 289.08, 290.17, 17186200),(4, '2022-08-19', 288.90, 289.25, 285.56, 286.15, 20557200),(4, '2022-08-22', 282.08, 282.46, 277.22, 277.75, 25061100),(4, '2022-08-23', 276.44, 278.86, 275.40, 276.44, 17527400),(4, '2022-08-24', 275.41, 277.23, 275.11, 275.79, 18137000),(4, '2022-08-25', 277.33, 279.02, 274.52, 278.85, 16583400),(4, '2022-08-26', 279.08, 280.34, 267.98, 268.09, 27532500),(4, '2022-08-29', 265.85, 267.40, 263.85, 265.23, 20338500),(4, '2022-08-30', 266.67, 267.05, 260.66, 262.97, 22767100)";
		PreparedStatement stmt_cmpany=con.prepareStatement(Insert_company);
		PreparedStatement stmt_sp=con.prepareStatement(Insert_SP);
		con.setAutoCommit(false);
		
		stmt_cmpany.executeUpdate();
		con.commit();
		System.out.println("Data inserted in Company table.");
		stmt_sp.executeUpdate();
		con.commit();
		System.out.println("Data inserted in SP table.");
	}
	
	public void delete() throws SQLException 
	{
		//TODO for delete query
		String DELETE_sp="DELETE FROM mydb.stockprice WHERE companyId=(select id from mydb.company c where c.name='GameStop' ) or  priceDate<cast('2022-08-20' as date)";
		con.setAutoCommit(false);
		PreparedStatement DLT_STMT=con.prepareStatement(DELETE_sp);
		
		DLT_STMT.executeUpdate();
		con.commit();
		System.out.println("Data Deleted from Stock Price Table where Price Date below 2022-08-20.");
	}
	
	public ResultSet queryOne() throws SQLException 
	{
		//TODO for returning ResultSet
		String sql1="SELECT name, annualRevenue, numEmployees FROM mydb.company where numEmployees>10000 or annualRevenue<1000000 order by name asc";
		Statement select_stmt=con.createStatement();
		
		return select_stmt.executeQuery(sql1);
	
	}
	
	public ResultSet queryTwo() throws SQLException 
	{
		//TODO for returning ResultSet
		
		String sql2="select c.name,c.ticker,min(sp.lowPrice) LOWPRICE,max(sp.highPrice) HIGHPRICE,avg(sp.closePrice) AVERAGE_CLOSEPRICE,avg(sp.volume) AVERAGE_VOLUME from company c,stockprice sp  where c.id=sp.companyId and sp.priceDate between '2022-08-22' AND '2022-08-26' order by AVERAGE_VOLUME";
		Statement select_stmt=con.createStatement();
		
		return select_stmt.executeQuery(sql2);
	}
	
	
	public ResultSet queryThree() throws SQLException 
	{
		//TODO for returning ResultSet
		
		String sql3="SELECT c.name, c.ticker, sp.closePrice from company c,stockprice sp where c.id=sp.companyId and (sp.closePrice<=(select avg(closePrice)*.90 from stockprice where priceDate between '2022-08-15' AND '2022-08-19') OR c.ticker is null) order by c.name asc";
		Statement select_stmt=con.createStatement();
		
		return select_stmt.executeQuery(sql3);
		
	}
	
	
	public void close() throws SQLException 
	{
		if (con != null) 
		{
			con.close();
		}
	}
	
	public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException
	{
		StringBuffer buf = new StringBuffer(5000);
		int rowCount = 0;
		if (rst == null)
			return "ERROR: No ResultSet";
		ResultSetMetaData meta = rst.getMetaData();
		buf.append("Total columns: " + meta.getColumnCount());
		buf.append('\n');
		if (meta.getColumnCount() > 0)
			buf.append(meta.getColumnName(1));
		for (int j = 2; j <= meta.getColumnCount(); j++)
			buf.append(", " + meta.getColumnName(j));
		buf.append('\n');
		while (rst.next())
		{
			if (rowCount < maxrows)
			{
				for (int j = 0; j < meta.getColumnCount(); j++)
				{
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
		buf.append(meta.getColumnName(1)+" ("+meta.getColumnLabel(1)+", "+meta.getColumnType(1)+"-"+meta.getColumnTypeName(1)+", "+meta.getColumnDisplaySize(1)+", "+meta.getPrecision(1)+", "+meta.getScale(1)+")");
		for (int j = 2; j <= meta.getColumnCount(); j++)
			buf.append(", "+meta.getColumnName(j)+" ("+meta.getColumnLabel(j)+","+meta.getColumnType(j)+"-"+meta.getColumnTypeName(j)+", "+meta.getColumnDisplaySize(j)+", "+meta.getPrecision(j)+", "+meta.getScale(j)+")");
		return buf.toString();
	}
	
}


