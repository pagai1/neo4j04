package algoPackage;

import java.util.Map;
import java.util.Map.Entry;
import java.lang.System;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class ExecutionEngine {

	private static GraphDatabaseService graphDB;
	private String rows = "";

	// Constructor
	/**
	 * Creates an instance of ExecutionEngine which allows to execute
	 * CYPHER-Commands to the Database.
	 * 
	 * @param inputGraphDB
	 */
	public ExecutionEngine(GraphDatabaseService inputGraphDB) {
		graphDB = inputGraphDB;
	}

	/**
	 * Runs a query to the current opened database.
	 * 
	 * @param query          - Inputquery as string
	 * @param verbose        - print result of query
	 * @param extraLinebreak - Output-lines receive an extra linebreak, sometimes
	 *                       good for better output.
	 * @param extraField     - Which character shall be used instead of linebreak,
	 *                       works only if extraLinebreak is false
	 */
	public void runQuery(String query, Boolean verbose, Boolean extraLinebreak, String extraField) {
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			long startTimeQuery = System.currentTimeMillis();
//			System.out.println("EXECUTING: \n" + query);

//			long startTimeClose = System.currentTimeMillis(); 
//			tx.close();
//			System.out.println("CLOSE TOOK: " + (System.currentTimeMillis() - startTimeClose) + "ms.");

			if (verbose) {
				printResult(tx.execute(query), extraLinebreak, extraField, false);
			} else {
				Result result = tx.execute(query);
				
				while (result.hasNext()) {
//					Map<String, Object> row = result.next();
//					for (Entry<String, Object> column : row.entrySet()) {
//						rows += column.getKey() + ": " + column.getValue();
//					}
					result.next();
				}
			}

//			while (result.hasNext()) {
//				Map<String, Object> row = result.next();
//				for (Entry<String, Object> column : row.entrySet()) {
////				System.out.println(column.getKey() + ": " + column.getValue());
//					rows += column.getKey() + ": " + column.getValue();
//				}
//				rows += "\n";
//			}

//			System.out.println("QUERY FINISHED.");
			System.out.println("EXECUTION TOOK: " + (System.currentTimeMillis() - startTimeQuery) + "ms.");
		}
	}

	/**
	 * Exports current opened DB to a given outputfile. The file will be written to
	 * the DBs import folder.
	 * 
	 * @param outputFile     - The String of the outputfilename.
	 * @param verbose        - Print out result or not.
	 * @param extraLinebreak - is there an additional linebreak necessary for the
	 *                       output? Helpful for multiline-results.
	 * @param extraField     - no idea anymore :D
	 */
	public void exportDBtoFile(String outputFile, Boolean verbose, Boolean extraLinebreak, String extraField) {
		String query = "CALL apoc.export.csv.all(\"" + outputFile + "\", {})";
		this.runQuery(query, verbose, extraLinebreak, extraField);
	}

	/**
	 * Prints out the result.
	 * @param result - the result parameter
	 * @param extraLinebreak - shall there be an extra line break
	 * @param extraField - if extra line break, which string shall be inserted. default = \n.
	 * @param onlySingle - if true only one outputline will be in the output.
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

	public void runShortestPathByCypher(String startNode, String endNode, boolean verbose) {
		String query = "MATCH (start:user {name: '" + startNode + "'}), (end:user {name: '" + endNode + "'})\n" + ""
				+ "CALL gds.beta.shortestPath.dijkstra.stream({\n" + "      nodeProjection: '*',\n" + "  relationshipProjection: {\n" + "    all: {\n"
				+ "      type: 'IS_FRIEND_OF',\n" + "      orientation: 'UNDIRECTED'\n" + "    }\n" + "  },\n" + "  sourceNode: id(start),\n"
				+ "  targetNode: id(end) })\n" + "YIELD nodeIds,sourceNode,targetNode,totalCost,index\n" + "RETURN\n" + "    index,\n"
				+ "    gds.util.asNode(sourceNode).name AS sourceNodeName,\n" + "    gds.util.asNode(targetNode).name AS targetNodeName,\n"
				+ "    totalCost,\n" + "    [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames\n" + "	 ORDER BY index";
		this.runQuery(query, verbose, true, "");
	}

}
