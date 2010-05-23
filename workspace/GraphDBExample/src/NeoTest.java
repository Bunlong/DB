import org.neo4j.graphdb.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;
 
/**
 * Example class that constructs a simple graph with message attributes and then prints them.
 */
public class NeoTest {
 
    public enum MyRelationshipTypes implements RelationshipType {
        KNOWS
    }
 
    public static void main(String[] args) {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase("var/base");
 
        Transaction tx = graphDb.beginTx();
        try {
            Node firstNode = graphDb.createNode();
            Node secondNode = graphDb.createNode();
            Relationship relationship = firstNode.createRelationshipTo(secondNode, MyRelationshipTypes.KNOWS);
 
            firstNode.setProperty("message", "Hello, ");
            secondNode.setProperty("message", "world!");
            relationship.setProperty("message", "brave Neo4j ");
            tx.success();
 
            System.out.print(firstNode.getProperty("message"));
            System.out.print(relationship.getProperty("message"));
            System.out.print(secondNode.getProperty("message"));
        }
        finally {
            tx.finish();
            graphDb.shutdown();
        }
    }
}