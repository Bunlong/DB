public class Main {

	// The experiment will be run twice in a row
	// and only the results of the second run
	// will be evaluated
	
	public static void main(String[] args) {
		JDBC DB = new JDBC();
		
		DB.createDatabase();
		System.out.println();
		
		DB.createTable();
		System.out.println();
		
		DB.createIndices();
		System.out.println();
		
		DB.loadData();
		System.out.println();
		
		DB.warmUpCache();
		System.out.println();
		
		DB.traverseGraph1(0);
		System.out.println();
		
		DB.traverseGraph2(0);
		System.out.println();

		DB.traverseGraph3(0);
		System.out.println();
		
		DB.traverseGraph4(0);
		System.out.println();
		
		DB.traverseGraph5(0);
		System.out.println();
	
		DB.dropDatabase();
		System.out.println();
	}
}
