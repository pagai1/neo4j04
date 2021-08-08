package algoPackage;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;

public class PageRankAnalysis {
	private static GraphDatabaseService graphDB;
	private String rows = "";

	public PageRankAnalysis(GraphDatabaseService inputGraphDB) {
		graphDB = inputGraphDB;
	}

	/**
	 * This method just shows all dbms.functions which can be executed.
	 * 
	 */
	public void listDBfunctions() {
		try (Transaction tx = graphDB.beginTx()) {
			Result result = tx.execute("call dbms.functions()");
			while (result.hasNext()) {
				System.out.println(result.next().toString());
			}
		}
	}

	public void warmUp( GraphDatabaseService graphDB) {
	  try ( Transaction tx = graphDB.beginTx()) {
	    for ( Node n : tx.getAllNodes()) {
	      n.getPropertyKeys();
	      for ( Relationship relationship : n.getRelationships()) {
	        relationship.getPropertyKeys();
	        relationship.getStartNode();
	      }
	    }
	  }
	  System.out.println("Warmed up and ready to go!");
	}
	/**
	 * Creates subgraph and executes PageRank algorithm on it.
	 * 
	 * @param graphName            - the graphname of the subgraph. name it as you
	 *                             want.
	 * @param mainLabel            - the label of the nodes which shall be added to
	 *                             the subgraph
	 * @param mainRelationship     - the main-relationship from which the subgraph
	 *                             shall be created.
	 * @param relationshipProperty - the property of the relation which shall be
	 *                             used for the weight of the relation.
	 * @param listConfig           - give additional output of the configuration of
	 *                             the subgraph
	 * @param verbose              - be more verbose. additional output about the
	 *                             things which are happening.
	 * @param maxLineOutput        - if verbose is true, limit the numbers of lines of the output
	 * 
	 */
	public void createSubgraphAndExecutePageRank(String graphName, String mainLabel, String mainRelationship, String relationshipProperty,
			Boolean listConfig, Boolean verbose, int maxLineOutput) {
//		warmUp(graphDB);
		

		
		Result result;
		String limitLines = "";
		String relProps = "";
		String relWeightProp = "";
		if (relationshipProperty != null) {
			relProps = ", { relationshipProperties: '" + relationshipProperty + "' }";
			relWeightProp = "relationshipWeightProperty: '" + relationshipProperty + "',";
		}
		if (maxLineOutput > 0) {
			limitLines = "LIMIT " + maxLineOutput;
		}
		
		try (Transaction tx = graphDB.beginTx()) {
			if (verbose) {
				System.out.println("CREATE TEMPORARY SUBGRAPH...");
			}
			String subGraphQuery = "CALL gds.graph.create(" + "  '" + graphName + "'," + "  '" + mainLabel + "'," + "  '" + mainRelationship + "'"
					+ relProps + ") YIELD graphName, nodeCount, relationshipCount,createMillis;";
			if (verbose) {
				printResult(tx.execute(subGraphQuery), false, "|", false);
			} else {
				result = tx.execute(subGraphQuery);
				result.accept(null);
//				result.next();

			}
			tx.close();
		}

		
		
//			Result resultCreation = tx.execute("CALL gds.graph.create(" + "  '" + graphName + "'," + "  '" + mainLabel + "'," + "  '"
//					+ mainRelationship + "'" + relProps + ") YIELD createMillis;");
//			while (resultCreation.hasNext()) {
//				System.out.println(resultCreation.next().toString());
//			}
		if (listConfig) {
			try (Transaction tx = graphDB.beginTx()) {
				String listConfigQuery = "CALL gds.graph.list( '" + graphName + "')\n"
						+ "YIELD graphName,database,nodeProjection, relationshipProjection, nodeQuery, relationshipQuery, nodeCount, relationshipCount, schema, degreeDistribution, density, creationTime, modificationTime, sizeInBytes, memoryUsage;";
				System.out.println("LIST CONFIG: ");
//				
				if (verbose) {
					printResult(tx.execute(listConfigQuery), false, "|", false);
				} else {
					result = tx.execute(listConfigQuery);
					result.next();
				}
				tx.close();
			}
//				Result listConfigResult = tx.execute("CALL gds.graph.list( graphName: '" + graphName
//						+ "') YIELD graphName,database,nodeProjection, relationshipProjection,  nodeQuery,  relationshipQuery,							  nodeCount,							  relationshipCount,							  schema,							  degreeDistribution,							  density,							  creationTime,							  modificationTime,							  sizeInBytes,							  memoryUsage;");
//				while (listConfigResult.hasNext()) {
//					System.out.println(listConfigResult.next().toString());
//				}
		}
		
		long start_time = System.currentTimeMillis();

		String query = "CALL gds.pageRank.stream('" + graphName + "', " + // Graph-Name
				"{ " + relWeightProp + " maxIterations: 100, dampingFactor: 0.85, concurrency: 1 }) " + // Configuration
				"YIELD nodeId, score " + "RETURN gds.util.asNode(nodeId).name AS name, score " + "ORDER BY score DESC " + limitLines;

		if (verbose) {
			System.out.println("EXECUTE PAGERANK... ");
			System.out.println(query);
		}
		try (Transaction tx = graphDB.beginTx()) {
			if (verbose) {
				printResult(tx.execute(query), false, "|", false);
			} else {
				result = tx.execute(query);
				result.next();
			}
			tx.close();
		}
			
		System.out.println("EXECUTION TOOK: " + (System.currentTimeMillis() - start_time) + "ms.");
		
		if (verbose) System.out.println("DROPPING SUBGRAPH");
		try (Transaction tx = graphDB.beginTx()) {
			result = tx.execute("CALL gds.graph.drop('SUBGRAPH')");
			result.next();
			tx.commit();
		} catch(Exception e) {
			System.out.println(e.getCause());
		}
	}

	/**
	 * Prints out the result.
	 * 
	 * @param result         - the result parameter
	 * @param extraLinebreak - shall there be an extra line break
	 * @param extraField     - if extra line break, which string shall be inserted.
	 *                       default = \n.
	 * @param onlySingle     - if true only one outputline will be in the output.
	 */
	private void printResult(Result result, boolean extraLinebreak, String extraField, boolean onlySingle) {

		String eLb = extraField;
		if (extraLinebreak) {
			eLb = "\n";
		}
		if (!onlySingle) {
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				for (Entry<String, Object> column : row.entrySet()) {
//				System.out.println(column.getKey() + ": " + column.getValue());
					rows += column.getKey() + ": " + column.getValue() + eLb;
				}
				rows += "\n";
			}

		} else {
			Map<String, Object> row = result.next();
			for (Entry<String, Object> column : row.entrySet()) {
//				System.out.println(column.getKey() + ": " + column.getValue());
				rows += column.getKey() + ": " + column.getValue() + eLb;
			}
			rows += "\n";
		}
		System.out.println(rows);
	}

}
