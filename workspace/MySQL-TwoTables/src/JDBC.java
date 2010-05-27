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
		    
		    String sql = "INSERT INTO node " +
		    "VALUES (0, 0)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (1, 1)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (2, 2)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (3, 3)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (4, 4)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (5, 5)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (6, 6)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (7, 7)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (8, 8)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (9, 0)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (10, 0)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (11, 11)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (12, 12)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (19, 19)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (25, 0)";
		    stmt.executeUpdate(sql);
		    
		    // Length 1
		    
		    sql = "INSERT INTO edge " +
		    "VALUES (0, 1)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO edge " +
		    "VALUES (0, 2)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO edge " +
		    "VALUES (0, 6)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO edge " +
		    "VALUES (0, 7)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO edge " +
		    "VALUES (0, 8)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO edge " +
		    "VALUES (0, 9)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO edge " +
		    "VALUES (0, 10)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO edge " +
		    "VALUES (0, 12)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO edge " +
		    "VALUES (0, 19)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO node " +
		    "VALUES (21, 21)";
		    stmt.executeUpdate(sql);
		    sql = "INSERT INTO edge " +
		    "VALUES (0, 25)";
		    stmt.executeUpdate(sql);
		    
		    // Length 2
		    
		    sql = "INSERT INTO edge " +
		    "VALUES (1, 3)";
		    stmt.executeUpdate(sql);
		    
		    // Length 3
		    
		    sql = "INSERT INTO edge " +
		    "VALUES (3, 4)";
		    stmt.executeUpdate(sql);
		    
		    // Length 4
		    
		    sql = "INSERT INTO edge " +
		    "VALUES (4, 5)";
		    stmt.executeUpdate(sql);
		    
		    // Length 5
		    
		    sql = "INSERT INTO edge " +
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
		    ")";
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int inV = rs.getInt("sink");

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
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 2...");
		    stmt = conn.createStatement();
		    
		    String sql = "SELECT b.inV FROM graph as a, graph as b " +
		    "WHERE a.inV=b.outV AND a.outV=" +
		    Integer.toString(root);
		    ResultSet rs = stmt.executeQuery(sql);
		    System.out.print("Visited vertices: ");
		    while (rs.next()) {
		    	int inV = rs.getInt("sink");

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
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 3...");
		    stmt = conn.createStatement();

		    String sql = "SELECT c.inV FROM graph as a, graph as b, " +
		    "graph as c WHERE a.inV=b.outV AND b.inV=c.outV AND a.outV=" +
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
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 4...");
		    stmt = conn.createStatement();

		    String sql = "SELECT d.inV FROM graph as a, graph as b, " +
		    "graph as c, graph as d WHERE a.inV=b.outV AND b.inV=c.outV " +
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
		    conn = DriverManager.getConnection(GTT_DB_URL, USER, PASS);
		    System.out.println("Connected to database successfully!");
		      
		    System.out.println("Creating statement for traversal " +
		    "of length 5...");
		    stmt = conn.createStatement();

		    String sql = "SELECT e.inV FROM graph as a, graph as b, " +
		    "graph as c, graph as d, graph as e WHERE a.inV=b.outV " +
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
		    	int inV = rs.getInt("id");

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
		    	int inV = rs.getInt("id");

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
		    	int inV = rs.getInt("id");

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
		    	int inV = rs.getInt("id");

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
		    	int inV = rs.getInt("id");

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