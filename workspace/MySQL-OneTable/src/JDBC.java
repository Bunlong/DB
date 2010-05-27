import java.sql.*;

public class JDBC {
	
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://localhost/";
	// GOT stands for GRAPH_ONE_TABLE
	static final String GOT_DB_URL = "jdbc:mysql://localhost/GOT";
	
	static final String USER = "sqluser";
	static final String PASS = "sqluserpw";
	
	// Database related operations
	
	public void createDatabase() {
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);

			System.out.println("Connecting to localhost...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			System.out.println("Connected to localhost successfully!");
			
			System.out.println("Creating database...");
			stmt = conn.createStatement();
      
			String sql = "CREATE DATABASE GOT";
			stmt.executeUpdate(sql);
			System.out.println("Database created successfully!");
		}
		catch (SQLException se) {
			se.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (stmt!=null)
					stmt.close();
			}
			catch (SQLException se) {
			}
			try {
				if (conn!=null)
					conn.close();
			}
			catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}
	
	public void dropDatabase() {
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(GOT_DB_URL, USER, PASS);
			System.out.println("Connected to database successfully!");
			
			System.out.println("Deleting database...");
			stmt = conn.createStatement();
			
			String sql = "DROP DATABASE GOT";
			stmt.executeUpdate(sql);
			System.out.println("Database deleted successfully!");
		}
		catch (SQLException se) {
			se.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (stmt!=null)
					conn.close();
			}
			catch (SQLException se) {
			}
			try {
				if (conn!=null)
					conn.close();
			}
			catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}	
	
	// Table related operations
	
	public void createTable() {
		Connection conn = null;
		Statement stmt = null;
		try	{
			Class.forName(JDBC_DRIVER);
			
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(GOT_DB_URL, USER, PASS);
			System.out.println("Connected to database successfully!");
			
			System.out.println("Creating table in database...");
			stmt = conn.createStatement();
	      
			String sql = "CREATE TABLE graph " +
			"(outV INT NOT NULL, " +
			" inV  INT NOT NULL)"; 
			
			stmt.executeUpdate(sql);
			System.out.println("Table created successfully!");
		}
		catch (SQLException se) {
			se.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (stmt!=null)
					conn.close();
			}
			catch (SQLException se) {
			}
			try {
				if (conn!=null)
					conn.close();
			}
			catch (SQLException se) {
				se.printStackTrace();
			}
		}	
	}
	
	public void createIndices() {
		Connection conn = null;
		Statement stmt = null;
		try	{
			Class.forName(JDBC_DRIVER);
			
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(GOT_DB_URL, USER, PASS);
			System.out.println("Connected to database successfully!");
			
			System.out.println("Creating indices in table...");
			stmt = conn.createStatement();
	      
			String sql1 = "CREATE INDEX outV_index " +
			"USING BTREE ON graph (outV)";
			
			String sql2 = "CREATE INDEX inV_index " +
			"USING BTREE ON graph (inV)"; 
			
			stmt.executeUpdate(sql1);
			stmt.executeUpdate(sql2);
			System.out.println("Indices created successfully!");
		}
		catch (SQLException se) {
			se.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (stmt!=null)
					conn.close();
			}
			catch (SQLException se) {
			}
			try {
				if (conn!=null)
					conn.close();
			}
			catch (SQLException se) {
				se.printStackTrace();
			}
		}	
	}
	
	public void loadData() {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GOT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Loading data...");
		    stmt = conn.createStatement();
		    
		    // Length 1
		    
		    String sql = "INSERT INTO graph " +
		    "VALUES (0, 1)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO graph " +
		    "VALUES (0, 2)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO graph " +
		    "VALUES (0, 6)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO graph " +
		    "VALUES (0, 7)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO graph " +
		    "VALUES (0, 8)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO graph " +
		    "VALUES (0, 9)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO graph " +
		    "VALUES (0, 10)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO graph " +
		    "VALUES (0, 12)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO graph " +
		    "VALUES (0, 19)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO graph " +
		    "VALUES (0, 25)";
		    stmt.executeUpdate(sql);
		    
		    // Length 2
		    
		    sql = "INSERT INTO graph " +
		    "VALUES (1, 3)";
		    stmt.executeUpdate(sql);
		    
		    // Length 3
		    
		    sql = "INSERT INTO graph " +
		    "VALUES (3, 4)";
		    stmt.executeUpdate(sql);
		    
		    // Length 4
		    
		    sql = "INSERT INTO graph " +
		    "VALUES (4, 5)";
		    stmt.executeUpdate(sql);
		    
		    // Length 5
		    
		    sql = "INSERT INTO graph " +
		    "VALUES (5, 11)";
		    stmt.executeUpdate(sql);
		    
		    System.out.println("Data loaded successfully!");
		}
		catch (SQLException se) {
		    se.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
			try {
				if (stmt!=null)
					conn.close();
			}
			catch (SQLException se) {
		    }
			try {
				if (conn!=null)
					conn.close();
		    }
			catch (SQLException se) {
				se.printStackTrace();
		    }
		}
	}
	
	public void warmUpCache() {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GOT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Warming up cache...");
		    stmt = conn.createStatement();

		    String sql = "SELECT * FROM graph";
		    ResultSet rs = stmt.executeQuery(sql);
		    
		    // XXX: All of the results must be iterated through
		    
		    System.out.println("Edges of graph: ");
		    while (rs.next()) {
		    	int outV = rs.getInt("outV");
		    	int inV = rs.getInt("inV");

		    	System.out.println(outV + " --> " + inV);
		    }
		    rs.close();
		}
		catch (SQLException se) {
			se.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
			try {
				if (stmt!=null)
					conn.close();
			}
			catch (SQLException se) {
		    }
			try {
				if(conn!=null)
					conn.close();
			}
			catch (SQLException se) {
				se.printStackTrace();
		    }
		}
	}
	
	// The number in the function name indicates
	// the length of the traversal
	
	public void traverseGraph1(int root) {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GOT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 1...");
		    stmt = conn.createStatement();

		    String sql = "SELECT a.inV FROM graph AS a WHERE a.outV=" +
		    Integer.toString(root);
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int inV = rs.getInt("inV");

		    	System.out.print(inV + " ");
		    }
		    System.out.println();
		    rs.close();
		}
		catch (SQLException se) {
			se.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
			try {
				if (stmt!=null)
					conn.close();
			}
			catch (SQLException se) {
		    }
			try {
				if(conn!=null)
					conn.close();
			}
			catch (SQLException se) {
				se.printStackTrace();
		    }
		}
	}
	
	public void traverseGraph2(int root) {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GOT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 2...");
		    stmt = conn.createStatement();

		    String sql = "SELECT b.inV FROM graph AS a, graph AS b " +
		    "WHERE a.inV=b.outV AND a.outV=" +
		    Integer.toString(root);
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int inV = rs.getInt("inV");

		    	System.out.print(inV + " ");
		    }
		    System.out.println();
		    rs.close();
		}
		catch (SQLException se) {
			se.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
			try {
				if (stmt!=null)
					conn.close();
			}
			catch (SQLException se) {
		    }
			try {
				if(conn!=null)
					conn.close();
			}
			catch (SQLException se) {
				se.printStackTrace();
		    }
		}
	}
	
	public void traverseGraph3(int root) {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GOT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 3...");
		    stmt = conn.createStatement();

		    String sql = "SELECT c.inV FROM graph AS a, graph AS b, " +
		    "graph AS c WHERE a.inV=b.outV AND b.inV=c.outV AND a.outV=" +
		    Integer.toString(root);
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int inV = rs.getInt("inV");

		    	System.out.print(inV + " ");
		    }
		    System.out.println();
		    rs.close();
		}
		catch (SQLException se) {
			se.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
			try {
				if (stmt!=null)
					conn.close();
			}
			catch (SQLException se) {
		    }
			try {
				if(conn!=null)
					conn.close();
			}
			catch (SQLException se) {
				se.printStackTrace();
		    }
		}
	}
	
	public void traverseGraph4(int root) {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GOT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 4...");
		    stmt = conn.createStatement();

		    String sql = "SELECT d.inV FROM graph AS a, graph AS b, " +
		    "graph AS c, graph AS d WHERE a.inV=b.outV AND b.inV=c.outV " +
		    "AND c.inV=d.outV AND a.outV=" +
		    Integer.toString(root);
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int inV = rs.getInt("inV");

		    	System.out.print(inV + " ");
		    }
		    System.out.println();
		    rs.close();
		}
		catch (SQLException se) {
			se.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
			try {
				if (stmt!=null)
					conn.close();
			}
			catch (SQLException se) {
		    }
			try {
				if(conn!=null)
					conn.close();
			}
			catch (SQLException se) {
				se.printStackTrace();
		    }
		}
	}
	
	public void traverseGraph5(int root) {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GOT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 5...");
		    stmt = conn.createStatement();

		    String sql = "SELECT e.inV FROM graph AS a, graph AS b, " +
		    "graph AS c, graph AS d, graph AS e WHERE a.inV=b.outV " +
		    "AND b.inV=c.outV AND c.inV=d.outV AND d.inV=e.outV AND a.outV=" +
		    Integer.toString(root);
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int inV = rs.getInt("inV");

		    	System.out.print(inV + " ");
		    }
		    System.out.println();
		    rs.close();
		}
		catch (SQLException se) {
			se.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
			try {
				if (stmt!=null)
					conn.close();
			}
			catch (SQLException se) {
		    }
			try {
				if(conn!=null)
					conn.close();
			}
			catch (SQLException se) {
				se.printStackTrace();
		    }
		}
	}
}