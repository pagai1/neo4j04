package algoPackage;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class PageRankAnalysis {
	private static GraphDatabaseService graphDB;

	public PageRankAnalysis(GraphDatabaseService inputGraphDB) {
		graphDB = inputGraphDB;
	}

	public void listDBfunctions() {
		try (Transaction tx = graphDB.beginTx()) {
			Result result = tx.execute("call dbms.functions()");
			while (result.hasNext()) {
				System.out.println(result.next().toString());
			}
		}
	}

	public void getPageRank() {
		try (Transaction tx = graphDB.beginTx()) {
			System.out.println("CREATE TEMPORARY SUBGRAPH...");
			Result resultCreation = tx.execute("CALL gds.graph.create(" + "  'ACTED_WITH-graph'," + "  'actor',"
					+ "  'ACTED_WITH'," + "  {" + "    relationshipProperties: 'count'" + "  }) YIELD createMillis;");
			while (resultCreation.hasNext()) {
				System.out.println(resultCreation.next().toString());
			}
			
			System.out.println("LIST CONFIG: ");
			Result listConfigResult = tx.execute(
					"CALL gds.graph.list( graphName: 'ACTED_WITH-graph') YIELD graphName,database,nodeProjection, relationshipProjection,  nodeQuery,  relationshipQuery,							  nodeCount,							  relationshipCount,							  schema,							  degreeDistribution,							  density,							  creationTime,							  modificationTime,							  sizeInBytes,							  memoryUsage;");
			while (listConfigResult.hasNext()) {
				System.out.println(listConfigResult.next().toString());
			}

			System.out.println("EXECUTE PAGERANK... ");
			@SuppressWarnings("unused")
			Result resultPageRank = tx.execute("CALL gds.pageRank.stream('ACTED_WITH-graph', " + // Graph-Name
					"{maxIterations: 20, dampingFactor: 0.85}) " + // Configuration
					"YIELD nodeId, score " + "RETURN gds.util.asNode(nodeId).name AS name, score "
					+ "ORDER BY score DESC LIMIT 10");


			tx.close();
		}

	}

	public void setProperties() {
		try (Transaction tx = graphDB.beginTx()) {
		
		tx.execute("CALL dbms.setConfigValue('dbms.security.procedures.unrestricted', 'apoc.*,gds.*')");
		tx.execute("CALL dbms.setConfigValue('dbms.security.procedures.whitelist', 'apoc.*,gds.*')");
		tx.commit();
//		tx.close();
		}
	}

}
