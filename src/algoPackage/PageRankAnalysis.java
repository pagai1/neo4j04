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

	/**
	 * Creates subgraph and executes PageRank algorithm on it.
	 * @param graphName
	 * @param mainLabel
	 * @param mainRelationship
	 * @param relationshipProperty
	 * @param listConfig
	 * @param verbose
	 */
	public void createSubgraphAndExecutePageRank(String graphName, String mainLabel, String mainRelationship, String relationshipProperty, Boolean listConfig,
			Boolean verbose, int maxLineOutput) {
		try (Transaction tx = graphDB.beginTx()) {
			String relProps = "";
			if (relationshipProperty != null) {
				relProps = ", { relationshipProperties: '" + relationshipProperty + "' }";
			}
			System.out.println("CREATE TEMPORARY SUBGRAPH...");
			Result resultCreation = tx.execute("CALL gds.graph.create(" + "  '" + graphName + "'," + "  '" + mainLabel + "'," + "  '"
					+ mainRelationship + "'" + relProps + ") YIELD createMillis;");
			while (resultCreation.hasNext()) {
				System.out.println(resultCreation.next().toString());
			}

			if (listConfig) {
				System.out.println("LIST CONFIG: ");
				Result listConfigResult = tx.execute("CALL gds.graph.list( graphName: '" + graphName
						+ "') YIELD graphName,database,nodeProjection, relationshipProjection,  nodeQuery,  relationshipQuery,							  nodeCount,							  relationshipCount,							  schema,							  degreeDistribution,							  density,							  creationTime,							  modificationTime,							  sizeInBytes,							  memoryUsage;");
				while (listConfigResult.hasNext()) {
					System.out.println(listConfigResult.next().toString());
				}
			}

			if (verbose)
				System.out.println("EXECUTE PAGERANK... ");
			@SuppressWarnings("unused")
			Result resultPageRank = tx.execute("CALL gds.pageRank.stream('" + graphName + "', " + // Graph-Name
					"{maxIterations: 100, dampingFactor: 0.85}) " + // Configuration
					"YIELD nodeId, score " + "RETURN gds.util.asNode(nodeId).name AS name, score " + "ORDER BY score DESC LIMIT " + maxLineOutput);
			while (resultPageRank.hasNext()) {
				if (verbose)
					System.out.println(resultPageRank.next().toString());
			}

			tx.close();
		}
	}

	/**
	 * This Method will call setConfigValue on DBMS. - CALL
	 * dbms.setConfigValue('dbms.security.procedures.unrestricted', 'apoc.*,gds.*')
	 * - CALL dbms.setConfigValue('dbms.security.procedures.whitelist',
	 * 'apoc.*,gds.*')
	 * 
	 * 
	 */
	public void setProperties() {
		try (Transaction tx = graphDB.beginTx()) {

			tx.execute("CALL dbms.setConfigValue('dbms.security.procedures.unrestricted', 'apoc.*,gds.*')");
			tx.execute("CALL dbms.setConfigValue('dbms.security.procedures.whitelist', 'apoc.*,gds.*')");
			tx.commit();
//		tx.close();
		}
	}

}
