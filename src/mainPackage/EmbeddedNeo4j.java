package mainPackage;

import algoPackage.*;

import java.lang.System;
import java.nio.file.*;
import java.util.Map;

//import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.io.IOException;

import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import dataPackage.dataController;

//import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class EmbeddedNeo4j {

	/**
	 * - FOR APOC, DBMS STUFF USE SETCONFIG WITH STRING MAP CONFIG. IT IS ENOUGH. -
	 * plugins-folder needed with apropriate jar-files. - neo4j.conf file not
	 * necessary
	 */

//	private static final String databaseConfig = "/home/pagai/graph-data/general_db_data/conf/neo4j.conf";
//	private static final File inputFolder = new File("/home/pagai/graph-data/");
//	private static final File importFolder = new File("/var/lib/neo4j/import/");
//	private static final File pluginsFolder = new File("/home/pagai/graph-data/general_db_data/plugins");

	private static Boolean cleanAndCreate = true;
	private static Boolean doAlgo = false;
	private static Boolean mainVerbose = false;
	// ########################################################
	// MOVIEDB
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/owndb01/").toPath();
////	private static final File inputFile = new File("/home/pagai/graph-data/tmdb.csv");
//	private static final File inputFile = new File("/home/pagai/graph-data/tmdb_fixed.csv");
//	private static String identifier = "movie";
//	private static enums.Labels mainLabel = enums.Labels.PERSON;
//	private static enums.RelationshipTypes mainRelation = enums.RelationshipTypes.ACTED_WITH;

//	 DEEZERDB
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/deezerdb/").toPath();
////	private static final File inputFile = new File("/home/pagai/_studium/_BA/_KN/graph-data/deezer_clean_data/both.csv");
//	private static final File inputFile = new File("/home/pagai/graph-data/pokec/soc-pokec-relationships_weighted.txt");
//	private static String identifier = "deezer";
//	private static enums.Labels mainLabel = enums.Labels.USER;
//	private static enums.RelationshipTypes mainRelation = enums.RelationshipTypes.IS_FRIEND_OF;

	// COOCCSDB
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/cooccsdatabase/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/cooccs.csv");
//	private static String identifier = "cooccs";
//	private static enums.Labels mainLabel = enums.Labels.SINGLE_NODE;
//	private static enums.RelationshipTypes mainRelation = enums.RelationshipTypes.IS_CONNECTED;

//	
//	// COOCCSDB_EXTERNAL
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/cooccsdatabase/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/cooccs.csv");
//	private static String identifier = "cooccs";
//	private static enums.Labels mainLabel = enums.Labels.SINGLE_NODE;
//	private static enums.RelationshipTypes mainRelation = enums.RelationshipTypes.IS_CONNECTED;

	// GENERAL TESTS
	private static final Path databaseDirectory = new File("/home/pagai/graph-data/general_tests/").toPath();
	private static final File inputFile = new File("/home/pagai/graph-data/general_tests.csv");
	private static String identifier = "general_tests";
	private static enums.Labels mainLabel = enums.Labels.SINGLE_NODE;
	private static enums.RelationshipTypes mainRelation = enums.RelationshipTypes.IS_CONNECTED;
	// ########################################################

	/**
	 * Setting config. Is used when opening the database and makes - export of
	 * graph-data to file possible - allows running of GDS algorithms and
	 * apoc-Algorithms via Cypher
	 **/

//	private static Map<String, String> config = MapUtil.stringMap("dbms.security.procedures.unrestricted", "gds.*","dbms.security.procedures.whitelist", "gds.*");
//	private static Map<String, String> config = MapUtil.stringMap("dbms.security.procedures.unrestricted", "gds.*,apoc.*", "dbms.security.procedures.whitelist", "gds.*,apoc.*");
//	private static Map<String, String> config = MapUtil.stringMap("apoc.export.file.enabled", "true");
	private static Map<String, String> config = MapUtil.stringMap("apoc.export.file.enabled", "true", "dbms.security.procedures.unrestricted",
			"gds.*,apoc.*", "dbms.security.procedures.whitelist", "gds.*,apoc.*", "dbms.logs.query.time_logging_enabled", "true",
			"dbms.logs.debug.level", "DEBUG", "dbms.tx_log.rotation.retention_policy", "500M size");

	private static GraphDatabaseService graphDB;
	private static DatabaseManagementService managementService;

	@SuppressWarnings("unused")
	private static ExecutionEngine ExEngine;
	@SuppressWarnings("unused")
	private static String outputFile = identifier + "db.csv";

	public static void main(final String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		System.out.print("BUILDING DATABASE..." + databaseDirectory + "\n");
		long buildTime = System.currentTimeMillis();
		managementService = new DatabaseManagementServiceBuilder(databaseDirectory).setConfigRaw(config).build();
		System.out.println("DONE IN " + (System.currentTimeMillis() - buildTime) + "ms.");
		graphDB = managementService.database("neo4j");
		registerShutdownHook(managementService);
		dataController myDataController = new dataController(graphDB);

// 		################ GENERAL TESTS ################

//		################ COOCCS DATABASE ################
//		managementService = new DatabaseManagementServiceBuilder(databaseDirectory).build();
//		managementService = new DatabaseManagementServiceBuilder(databaseDirectory).loadPropertiesFromFile(databaseConfig).build();

//		##################################################
//		rounds is here taken to use increasing amount of data and make loops to keep it running to test different sizes. Used for example in general tests.
		for (int round = 25000; round < 1000001; round = round + 25000) {
//			System.out.println("######## STARTING WITH ROUND: " + rounds);
			if (mainVerbose) System.out.println("######## STARTING ############");

			int lineLimit = 0;
			if (cleanAndCreate) {
				Boolean clearAndCreateIndizesVerbose = false;
				myDataController.clearDB(graphDB, clearAndCreateIndizesVerbose, 100000);
//				myDataController.clearDBByCypher(graphDB, clearAndCreateIndizesVerbose);
				myDataController.clearIndexes(graphDB, clearAndCreateIndizesVerbose);
				myDataController.createIndexes(graphDB, identifier, clearAndCreateIndizesVerbose);

				long startTime2 = System.currentTimeMillis();

				if (identifier.equals("movie")) {
					myDataController.loadDataFromCSVFile(inputFile, ",", graphDB, false, 1000);
//					myDataController.printAll(graphDB);
				}

				if (identifier.equals("deezer")) {
					myDataController.runDeezerImportByMethods(inputFile, 10000000, true, true, 1000000);
					myDataController.runDeezerImportByCypher(inputFile, 10000, true, true, 0);
//		 			myDataController.printAll(graphDB);
				}

				if (identifier.equals("cooccs")) {
					myDataController.runCooccsImportByMethods(graphDB, inputFile, false, 0);
				}

				if (identifier.equals("general_tests")) {
//					myDataController.clearDB(graphDB, clearAndCreateIndizesVerbose, 0);
//					myDataController.createIndexes(graphDB, identifier, clearAndCreateIndizesVerbose);
					myDataController.createNodes(round, mainLabel, false);
//					myDataController.createIndexes(graphDB, identifier);
//					myDataController.createNodes(amount);
//					myDataController.makeCompleteGraph();
//					myDataController.printAll(graphDB);
				}
				if (mainVerbose) {
					System.out.println("FINISHED IMPORT AFTER " + (System.currentTimeMillis() - startTime2) + "ms.");
					try (Transaction tx = graphDB.beginTx()) {
						System.out.println("########### DATABASE CONTENT ##########");
						System.out.println("NODES: " + tx.getAllNodes().stream().count());
						System.out.println("EDGES: " + tx.getAllRelationships().stream().count());
					}
				}

			}
//			System.out.println("######## END WITH LINES: " + rounds);

		}

		if (doAlgo) {
			/**
			 * SHORTEST PATH
			 */

			ShortestPathAnalysis SPAnalysis = new ShortestPathAnalysis(graphDB);
//		SPAnalysis.getShortestPath(enums.Labels.USER, "5", enums.Labels.USER, "134", enums.RelationshipTypes.IS_FRIEND_OF);
			SPAnalysis.getAllShortestPaths(mainLabel, mainRelation, false);
//		ShortestPathAnalysis SPAnalysis = new ShortestPathAnalysis(graphDB);
//		SPAnalysis.getShortestPath(enums.Labels.ACTOR, "Forest Whitaker", enums.Labels.ACTOR, "Miles Teller");
//		SPAnalysis.getAllShortestPaths(enums.Labels.SINGLE_NODE,enums.RelationshipTypes.IS_CONNECTED);

			/**
			 * PAGERANK
			 */
//		PageRankAnalysis PRAnalysis= new PageRankAnalysis(graphDB);
//		PRAnalysis.listDBfunctions();
//		PRAnalysis.setProperties();
//		PRAnalysis.getPageRank();

			/**
			 * CYPHERS
			 */
//		@SuppressWarnings("unused")
//		String query = "CALL apoc.export.csv.all(\"" + outputFile + "\", {})";
//		@SuppressWarnings("unused")
//		String query = "CALL gds.list();";
//		@SuppressWarnings("unused")
//		String query = "CALL apoc.help(\"apoc\");";

//		String call_schema = "CALL db.schema()";

			/**
			 * The following Strings create Subgraphs for GDS-Algorithmtests
			 */

//		@SuppressWarnings("unused")
//		String createGraph = "CALL gds.graph.create( " +
//				"  'SUBGRAPH', \n"+ // temporary graph name
//				"  'SINGLE_NODE', \n" +  // Nodelabel
//				"  'IS_CONNECTED', \n" +  // Relation 
//				"  {relationshipProperties: 'cost'})\n"	+ 
//				"YIELD graphName, nodeCount, relationshipCount;\n";
//
			@SuppressWarnings("unused")
			String createSubGraphMovieDB = "CALL gds.graph.create( 'SUBGRAPH', \n" + // temporary graph name
					"  'PERSON', \n" + // Nodelabel
					"  'ACTED_WITH')"; // Relation

			@SuppressWarnings("unused")
			String createSubGraphTextProcessing = "CALL gds.graph.create( " + "  'SUBGRAPH', \n" + // temporary graph name
					"  'SINGLE_NODE', \n" + // Nodelabel
					"  'IS_CONNECTED')"; // Relation

			@SuppressWarnings("unused")
			String createSubGraphDeezer = "CALL gds.graph.create( " + "  'SUBGRAPH', \n" + // temporary graph name
					"  'USER', \n" + // Nodelabel
					"  'IS_FRIEND_OF')"; // Relation

			@SuppressWarnings("unused")
			String createGraphALL = "CALL gds.graph.create( 'SUBGRAPH_ALL', '*', '*') ";
//
//		@SuppressWarnings("unused")
//		String createGraphByCypher = "CALL gds.graph.create.cypher('SUBGRAPH','MATCH (n) RETURN id(n) AS id','MATCH (n)-[e]-(m) RETURN id(n) AS source, e.weight AS weight, id(m) AS target, type(e) as type')";
//				

			@SuppressWarnings("unused")
			String createFullGraphByCypher = "CALL gds.graph.create.cypher( " + "  'SUBGRAPH', \n" + // temporary graph name
					"  'MATCH (n) RETURN id(n) AS id', \n" + // Nodelabel
					"  'MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target, type(r) as type')"; // Relation

			/**
			 * CALL OF ALGORITHMS ON GRAPHS/SUBGRAPHS
			 */

//		@SuppressWarnings("unused")
//		String betweenness = "CALL gds.betweenness.stream(\n" + 
//				" 'SUBGRAPH'\n" + 
//				")\n" + 
//				"YIELD\n" + 
//				"  nodeId,\n" + 
//				"  score \n" +
//				"RETURN gds.util.asNode(nodeId).name AS Name, score\n"+
//				"ORDER BY score ASC;";				
//

//		@SuppressWarnings("unused")
//		String allShortestPaths = "CALL gds.alpha.allShortestPaths.stream(\n"
//				+ "{nodeProjection: 'USER',\n"
//				+ " relationshipProjection: {\n"
//				+ "		IS_FRIEND_OF: {\n"
//				+ "			type: 'IS_FRIEND_OF',\n"
//				+ "			properties: 'weight'\n"
//				+ "		}\n"
//				+ "	},\n"
//				+ "relationshipWeightProperty: 'weight'})\n"
//				+ "YIELD sourceNodeId, targetNodeId, distance\n"
//				+ "WITH gds.util.asNode(sourceNodeId) AS sourceNode, gds.util.asNode(targetNodeId) AS targetNode, distance AS value\n"
//				+ "RETURN sourceNode.name AS source, targetNode.name AS target, value\n" + 
//				"ORDER BY value DESC, source ASC, target ASC\n" + 
//				"LIMIT 10";

			@SuppressWarnings("unused")
			String allShortestPaths = "CALL gds.alpha.allShortestPaths.stream(\n" + "{nodeProjection: 'USER',\n" + " relationshipProjection: {\n"
					+ "		IS_FRIEND_OF: {\n" + "			type: 'IS_FRIEND_OF',\n" + "			properties: 'weight'\n" + "		}\n" + "	},\n"
					+ "relationshipWeightProperty: 'weight'})\n" + "YIELD sourceNodeId, targetNodeId, distance\n"
					+ "WITH sourceNodeId, targetNodeId, distance \n"
//				+ "WHERE gds.util.isFinite(distance) = true \n"
					+ "MATCH (source:USER) WHERE id(source) = sourceNodeId \n" + "MATCH (target:USER) WHERE id(target) = targetNodeId \n"
					+ "WITH source, target, distance WHERE source <> target \n" + "RETURN source.name AS source, target.name AS target, distance\n";
//				+ "ORDER BY distance ASC, source ASC, target ASC\n"; 
//				+ "LIMIT 10";

			@SuppressWarnings("unused")
			String simRank = "CALL gds.nodeSimilarity.stream('SUBGRAPH') " + "YIELD node1, node2, similarity \n"
					+ "RETURN gds.util.asNode(node1).name AS n1, gds.util.asNode(node2).name as n2, similarity \n"
					+ "ORDER BY similarity DESCENDING, n1, n2";

			@SuppressWarnings("unused")
			String pageRank = "CALL gds.pageRank.stream('SUBGRAPH', { maxIterations: 100 })\n" + "YIELD nodeId, score \n"
					+ "RETURN gds.util.asNode(nodeId).name AS name, score\n" + "ORDER BY score DESC " + "LIMIT 25";

			@SuppressWarnings("unused")
			String pageRankAll = "CALL gds.pageRank.stream('SUBGRAPH_ALL', { maxIterations: 100 })\n" + "YIELD nodeId, score \n"
					+ "RETURN gds.util.asNode(nodeId).name AS name, score\n" + "ORDER BY score DESC " + "LIMIT 25";

			@SuppressWarnings("unused")
			String pageRankWeighted = "CALL gds.pageRank.stream('SUBGRAPH') YIELD nodeId, score AS pageRank\n"
					+ "WITH gds.util.asNode(nodeId) AS n, pageRank\n" + "MATCH (n)-[i:IS_CONNECTED]-()\n"
					+ "RETURN n.name AS name, pageRank, count(i) AS degree, sum(i.count) AS weightedDegree\n"
					+ "ORDER BY weightedDegree DESC LIMIT 25";

			@SuppressWarnings("unused")
			String hits = "CALL gds.alpha.hits.stream('SUBGRAPH', {hitsIterations: 100}) \n" + "YIELD nodeId,values\n"
					+ "RETURN gds.util.asNode(nodeId).name AS Name, values.auth AS auth, values.hub as hub \n" + "ORDER BY hub DESC";

			@SuppressWarnings("unused")
			String betweennessCentrality = "CALL gds.betweenness.stream(\n" + "  graphName: 'SUBGRAPH',\n" + "  configuration: 'SUBGRAPH'\n" + ")\n"
					+ "YIELD nodeId,score";

			@SuppressWarnings("unused")
			String get_herr = "MATCH (n:SINGLE_NODE)-[rel:IS_CONNECTED]->(m:SINGLE_NODE) RETURN m.name, count(n)";

////	

			ExEngine = new ExecutionEngine(graphDB);
////		ExEngine.runQuery(createGraphByCypher, true, false);
//		String returnAllNodes = "MATCH (n) RETURN count(*)";
//		ExEngine.runQuery(returnAllNodes, true, false);
//		String returnAllEdges = "MATCH ()-[r]->() RETURN count(r)";
//		ExEngine.runQuery(returnAllEdges, true, false);
//
////		ExEngine.runQuery(call_schema, true, false);

			/**
			 * SUBGRAPHCREATION
			 */
//		ExEngine.runQuery(createFullGraphByCypher, true, true, " ");
//		ExEngine.runQuery(createSubGraphMovieDB, true, false, " ");
//		ExEngine.runQuery(createSubGraphDeezer, true, false, " ");

//		ExEngine.runQuery(createGraphALL, true ,false, "");

			/**
			 * ALGOS
			 */
			ExEngine.runQuery(allShortestPaths, false, false, " ");
//		ExEngine.runQuery(pageRank, true, true);

//
////		ExEngine.runQuery(betweenness, true, false);
//		ExEngine.runQuery(pageRankAll, true, false);

//		ExEngine.runQuery(pageRankWeighted, true, false);
////		ExEngine.runQuery(get_herr, true, true);
//		ExEngine.runQuery(hits, true, false, " ");
//		ExEngine.runQuery(simRank, true, false, "|");
//		ExEngine.exportDBtoFile(outputFile, true, false);
//
////		String endNode = "bums";
////		String startNode = "bums";
////		ExEngine.runShortestPathByCypher(startNode, endNode, true);
//		
		}
		managementService.shutdown();

//		managementServiceDeezer.shutdown();
		if (mainVerbose) System.out.println("SHUTTING DOWN AFTER " + (System.currentTimeMillis() - startTime) + "ms.");
	}

//	@SuppressWarnings("unused")
	private static void registerShutdownHook(final DatabaseManagementService managementService) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				managementService.shutdown();
			}
		});
	}
}