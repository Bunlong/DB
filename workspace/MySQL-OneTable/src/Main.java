public class Main {

	public static void main(String[] args) {
		JDBC DB = new JDBC();
		
		DB.createDatabase();
		System.out.println();
		
		DB.createTable();
		System.out.println();
		
		DB.createIndices();
		System.out.println();
		
		DB.dropDatabase();
		System.out.println();
	}
}
