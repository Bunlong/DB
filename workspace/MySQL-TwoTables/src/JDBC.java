import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBC {
	
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://localhost/";
	// GTT stands for GRAPH_TWO_TABLES
	static final String GTT_DB_URL = "jdbc:mysql://localhost/GTT";
	
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
      
			String sql = "CREATE DATABASE GTT";
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
			conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
			System.out.println("Connected to database successfully!");
			
			System.out.println("Deleting database...");
			stmt = conn.createStatement();
			
			String sql = "DROP DATABASE GTT";
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
	
	public void createTables() {
		Connection conn = null;
		Statement stmt = null;
		try	{
			Class.forName(JDBC_DRIVER);
			
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
			System.out.println("Connected to database successfully!");
			
			System.out.println("Creating tables in database...");
			stmt = conn.createStatement();
	      
			String sql = "CREATE TABLE node " +
			"(id INT NOT NULL, " +
			" payload INT NOT NULL," +
			" PRIMARY KEY (id))";
			stmt.executeUpdate(sql);
			
			sql = "CREATE TABLE edge " +
			"(source INT NOT NULL, " +
			" sink INT NOT NULL," +
			" FOREIGN KEY (source) REFERENCES node(id)," +
			" FOREIGN KEY (sink) REFERENCES node(id))";
			stmt.executeUpdate(sql);
			System.out.println("Tables created successfully!");
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
	
	// XXX: Not sure about this
	
	public void createIndices() {
		Connection conn = null;
		Statement stmt = null;
		try	{
			Class.forName(JDBC_DRIVER);
			
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
			System.out.println("Connected to database successfully!");
			
			System.out.println("Creating indices in tables...");
			stmt = conn.createStatement();
	      
			String sql1 = "CREATE INDEX source_index " +
			"USING BTREE ON edge (source)";
			
			String sql2 = "CREATE INDEX sink_index " +
			"USING BTREE ON edge (sink)"; 
			
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
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Loading data...");
		    stmt = conn.createStatement();
		    
		    // Load data from file
		    
		    BufferedReader in = new BufferedReader(new FileReader("graph.txt"));
		    String line = in.readLine();
		    while (line != null && line.length() != 0) {
		    	String[] a = line.split(" ");
			    int id = Integer.parseInt(a[0]);
			    int payload = Integer.parseInt(a[1]);
			    
			    stmt = conn.createStatement();
			    String sql = "INSERT INTO node " +
			    "VALUES (" +
			    Integer.toString(id) +
			    ", " +
			    Integer.toString(payload) +
			    ")";
			    stmt.executeUpdate(sql);
			    line = in.readLine();
		    }
		    
		    line = in.readLine();
		    while (line != null) {
		    	String[] a = line.split(" ");
			    int source = Integer.parseInt(a[0]);
			    int sink = Integer.parseInt(a[1]);
			    
			    stmt = conn.createStatement();
			    String sql = "INSERT INTO edge " +
			    "VALUES (" +
			    Integer.toString(source) +
			    ", " +
			    Integer.toString(sink) +
			    ")";
			    stmt.executeUpdate(sql);
			    line = in.readLine();
		    }
		    
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
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Warming up cache...");
		    stmt = conn.createStatement();

		    String sql = "SELECT * FROM edge";
		    ResultSet rs = stmt.executeQuery(sql);
		    
		    // XXX: All of the results must be iterated through
		    
		    System.out.println("Edges of graph: ");
		    while (rs.next()) {
		    	int source = rs.getInt("source");
		    	int sink = rs.getInt("sink");

		    	System.out.println(source + " --> " + sink);
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
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 1...");
		    stmt = conn.createStatement();

		    String sql = "SELECT a.sink FROM edge AS a " + 
		    "JOIN node AS b ON (a.source=b.id AND a.source=" +
		    Integer.toString(root) +
		    ") JOIN node AS c ON (a.sink=c.id)";
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int sink = rs.getInt("sink");

		    	System.out.print(sink + " ");
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
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 2...");
		    stmt = conn.createStatement();

		    String sql = "SELECT a.sink FROM edge AS a " + 
		    "JOIN node AS b ON (a.source=b.id AND a.source=" +
		    Integer.toString(root) +
		    ") JOIN node AS c ON (a.sink=c.id)";
		    
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int sink = rs.getInt("sink");

		    	String sql1 = "SELECT a.sink FROM edge AS a " + 
			    "JOIN node AS b ON (a.source=b.id AND a.source=" +
			    Integer.toString(sink) +
			    ") JOIN node AS c ON (a.sink=c.id)";
		    	
		    	Statement stmt1 = null;
		    	stmt1 = conn.createStatement();
		    	ResultSet rs1 = stmt1.executeQuery(sql1);
		    	
		    	while (rs1.next()) {
			    	int sink1 = rs1.getInt("sink");   	
			    	System.out.print(sink1 + " ");
		    	}
		    	rs1.close();
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
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 3...");
		    stmt = conn.createStatement();

		    String sql = "SELECT a.sink FROM edge AS a " + 
		    "JOIN node AS b ON (a.source=b.id AND a.source=" +
		    Integer.toString(root) +
		    ") JOIN node AS c ON (a.sink=c.id)";
		    
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int sink = rs.getInt("sink");

		    	String sql1 = "SELECT a.sink FROM edge AS a " + 
			    "JOIN node AS b ON (a.source=b.id AND a.source=" +
			    Integer.toString(sink) +
			    ") JOIN node AS c ON (a.sink=c.id)";
		    	
		    	Statement stmt1 = null;
		    	stmt1 = conn.createStatement();
		    	ResultSet rs1 = stmt1.executeQuery(sql1);
		    	
		    	while (rs1.next()) {
			    	int sink1 = rs1.getInt("sink");
			    	
			    	String sql2 = "SELECT a.sink FROM edge AS a " + 
				    "JOIN node AS b ON (a.source=b.id AND a.source=" +
				    Integer.toString(sink1) +
				    ") JOIN node AS c ON (a.sink=c.id)";
			    	
			    	Statement stmt2 = null;
			    	stmt2 = conn.createStatement();
			    	ResultSet rs2 = stmt2.executeQuery(sql2);
			    	
			    	while (rs2.next()) {
			    		int sink2 = rs2.getInt("sink");
			    		
			    		System.out.print(sink2 + " ");
			    	}
			    	rs2.close();
		    	}
		    	rs1.close();
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
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 4...");
		    stmt = conn.createStatement();

		    String sql = "SELECT a.sink FROM edge AS a " + 
		    "JOIN node AS b ON (a.source=b.id AND a.source=" +
		    Integer.toString(root) +
		    ") JOIN node AS c ON (a.sink=c.id)";
		    
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int sink = rs.getInt("sink");

		    	String sql1 = "SELECT a.sink FROM edge AS a " + 
			    "JOIN node AS b ON (a.source=b.id AND a.source=" +
			    Integer.toString(sink) +
			    ") JOIN node AS c ON (a.sink=c.id)";
		    	
		    	Statement stmt1 = null;
		    	stmt1 = conn.createStatement();
		    	ResultSet rs1 = stmt1.executeQuery(sql1);
		    	
		    	while (rs1.next()) {
			    	int sink1 = rs1.getInt("sink");
			    	
			    	String sql2 = "SELECT a.sink FROM edge AS a " + 
				    "JOIN node AS b ON (a.source=b.id AND a.source=" +
				    Integer.toString(sink1) +
				    ") JOIN node AS c ON (a.sink=c.id)";
			    	
			    	Statement stmt2 = null;
			    	stmt2 = conn.createStatement();
			    	ResultSet rs2 = stmt2.executeQuery(sql2);
			    	
			    	while (rs2.next()) {
			    		int sink2 = rs2.getInt("sink");
			    		
			    		String sql3 = "SELECT a.sink FROM edge AS a " + 
					    "JOIN node AS b ON (a.source=b.id AND a.source=" +
					    Integer.toString(sink2) +
					    ") JOIN node AS c ON (a.sink=c.id)";
			    		
			    		Statement stmt3 = null;
				    	stmt3 = conn.createStatement();
				    	ResultSet rs3 = stmt3.executeQuery(sql3);
				    	
				    	while (rs3.next()) {
				    		int sink3 = rs3.getInt("sink");
			    		
				    		System.out.print(sink3 + " ");
				    	}
			    	}
			    	rs2.close();
		    	}
		    	rs1.close();
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
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 5...");
		    stmt = conn.createStatement();

		    String sql = "SELECT a.sink FROM edge AS a " + 
		    "JOIN node AS b ON (a.source=b.id AND a.source=" +
		    Integer.toString(root) +
		    ") JOIN node AS c ON (a.sink=c.id)";
		    
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int sink = rs.getInt("sink");

		    	String sql1 = "SELECT a.sink FROM edge AS a " + 
			    "JOIN node AS b ON (a.source=b.id AND a.source=" +
			    Integer.toString(sink) +
			    ") JOIN node AS c ON (a.sink=c.id)";
		    	
		    	Statement stmt1 = null;
		    	stmt1 = conn.createStatement();
		    	ResultSet rs1 = stmt1.executeQuery(sql1);
		    	
		    	while (rs1.next()) {
			    	int sink1 = rs1.getInt("sink");
			    	
			    	String sql2 = "SELECT a.sink FROM edge AS a " + 
				    "JOIN node AS b ON (a.source=b.id AND a.source=" +
				    Integer.toString(sink1) +
				    ") JOIN node AS c ON (a.sink=c.id)";
			    	
			    	Statement stmt2 = null;
			    	stmt2 = conn.createStatement();
			    	ResultSet rs2 = stmt2.executeQuery(sql2);
			    	
			    	while (rs2.next()) {
			    		int sink2 = rs2.getInt("sink");
			    		
			    		String sql3 = "SELECT a.sink FROM edge AS a " + 
					    "JOIN node AS b ON (a.source=b.id AND a.source=" +
					    Integer.toString(sink2) +
					    ") JOIN node AS c ON (a.sink=c.id)";
			    		
			    		Statement stmt3 = null;
				    	stmt3 = conn.createStatement();
				    	ResultSet rs3 = stmt3.executeQuery(sql3);
				    	
				    	while (rs3.next()) {
				    		int sink3 = rs3.getInt("sink");
				    		
				    		String sql4 = "SELECT a.sink FROM edge AS a " + 
						    "JOIN node AS b ON (a.source=b.id AND a.source=" +
						    Integer.toString(sink3) +
						    ") JOIN node AS c ON (a.sink=c.id)";
				    		
				    		Statement stmt4 = null;
					    	stmt4 = conn.createStatement();
					    	ResultSet rs4 = stmt4.executeQuery(sql4);
					    	
					    	while (rs4.next()) {
					    		int sink4 = rs4.getInt("sink");
				    		
					    		System.out.print(sink4 + " ");
					    	}
				    	}
			    	}
			    	rs2.close();
		    	}
		    	rs1.close();
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
	
	public void findOrphans() {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for finding " +
		    "orphan nodes...");
		    stmt = conn.createStatement();

		    String sql = "SELECT id FROM node " + 
		    "WHERE node.id NOT IN (SELECT source FROM edge)" +
		    "AND node.id NOT IN (SELECT sink FROM edge)";
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Orphan nodes: ");
		    while (rs.next()) {
		    	int sink = rs.getInt("id");

		    	System.out.print(sink + " ");
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
	
	public void payloadEqualTo(int value) {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for finding " +
		    "nodes with payload equal to a value...");
		    stmt = conn.createStatement();

		    String sql = "SELECT id FROM node " + 
		    "WHERE node.payload=" +
		    Integer.toString(value);
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Nodes: ");
		    while (rs.next()) {
		    	int sink = rs.getInt("id");

		    	System.out.print(sink + " ");
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
	
	public void payloadLessThan(int value) {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for finding " +
		    "nodes with payload less than a value...");
		    stmt = conn.createStatement();

		    String sql = "SELECT id FROM node " + 
		    "WHERE node.payload<" +
		    Integer.toString(value);
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Nodes: ");
		    while (rs.next()) {
		    	int sink = rs.getInt("id");

		    	System.out.print(sink + " ");
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
	
	public void payloadGreaterThan(int value) {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for finding " +
		    "nodes with payload greater than a value...");
		    stmt = conn.createStatement();

		    String sql = "SELECT id FROM node " + 
		    "WHERE node.payload>" +
		    Integer.toString(value);
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Nodes: ");
		    while (rs.next()) {
		    	int sink = rs.getInt("id");

		    	System.out.print(sink + " ");
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
	
	public void payloadContains(int value) {
		Connection conn = null;
		Statement stmt = null;
		try {
		    Class.forName(JDBC_DRIVER);

		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for finding " +
		    "nodes with payload that contains a value...");
		    stmt = conn.createStatement();

		    String sql = "SELECT id FROM node " + 
		    "WHERE node.payload LIKE '%" +
		    Integer.toString(value) +
		    "%'";
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Nodes: ");
		    while (rs.next()) {
		    	int sink = rs.getInt("id");

		    	System.out.print(sink + " ");
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