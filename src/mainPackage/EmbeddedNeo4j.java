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

	// ########################################################
	// MOVIEDB
	private static final Path databaseDirectory = new File("/home/pagai/graph-data/owndb01/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/tmdb.csv");
	private static final File inputFile = new File("/home/pagai/graph-data/tmdb_fixed.csv");
	private static String identifier = "movie";

	// DEEZERDB
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/deezerdb/").toPath();
//	private static final File inputFile = new File("/home/pagai/_studium/_BA/_KN/graph-data/deezer_clean_data/both.csv");
//	private static String identifier = "deezer";

	// COOCCSDB
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/cooccsdatabase/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/cooccs.csv");
//	private static String identifier = "cooccs";
//	
//	// COOCCSDB_EXTERNAL
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/cooccsdatabase/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/cooccs.csv");
//	private static String identifier = "cooccs";

//	// GENERAL TESTS
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/general_tests/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/general_tests.csv");
//	private static String identifier = "general_tests";

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
			"gds.*,apoc.*", "dbms.security.procedures.whitelist", "gds.*,apoc.*");

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

//		for (int amount = 250000; amount <= 10000001; amount = amount + 250000) {
//			myDataController.clearDB(graphDB,false);
//			myDataController.clearIndexes(graphDB, false);
//			myDataController.createNodes(amount, false);
//		}

//		myDataController.createIndexes(graphDB, identifier);
//		myDataController.createNodes(amount);
//		myDataController.makeCompleteGraph();
//		myDataController.printAll(graphDB);


//		################ COOCCS DATABASE ################
//		managementService = new DatabaseManagementServiceBuilder(databaseDirectory).build();
//		managementService = new DatabaseManagementServiceBuilder(databaseDirectory).loadPropertiesFromFile(databaseConfig).build();

//		##################################################
		for (int krachbumm = 0; krachbumm < 1; krachbumm = krachbumm + 10) {
			System.out.println("######## STARTING WITH LINES: " + krachbumm);
			int lineLimit = 0;
			if (cleanAndCreate) {
				myDataController.clearDB(graphDB, false);
				myDataController.clearIndexes(graphDB, false);
				myDataController.createIndexes(graphDB, identifier, false);

				long startTime2 = System.currentTimeMillis();

				if (identifier.equals("movie")) {
					myDataController.loadDataFromCSVFile(inputFile, ",", graphDB, false, 0);
				}

				if (identifier.equals("deezer")) {
//				myDataController.runDeezerImportByMethods(inputFile, krachbumm);
				myDataController.runDeezerImportByCypher(inputFile, krachbumm);
//		 		myDataController.printAll(graphDB);
				}

				if (identifier.equals("cooccs")) {
					myDataController.runCooccsImportByMethods(inputFile, false);
				}

				if (identifier.equals("general_tests")) {
					for (int i = lineLimit; i < 10002; i = i++) {
						System.out.println("################### RUNNING WITH " + lineLimit + " LINES.");
						myDataController.loadDataFromCSVFile(inputFile, ",", graphDB, false, i);

					}
				}
				System.out.println("FINISHED IMPORT AFTER " + (System.currentTimeMillis() - startTime2) + "ms.");
				try (Transaction tx = graphDB.beginTx()) {
					System.out.println("NODES: " + tx.getAllNodes().stream().count());
					System.out.println("EDGES: " + tx.getAllRelationships().stream().count());
				}

			}
			System.out.println("######## END WITH LINES: " + krachbumm);

		}

//		myMovieDataController.printAll(graphDB_movies);
//		
//		ShortestPathAnalysis SPAnalysis = new ShortestPathAnalysis(graphDB);
//		SPAnalysis.getShortestPath(enums.Labels.USER, "5", enums.Labels.USER, "134", enums.RelationshipTypes.IS_FRIEND_OF);
//		SPAnalysis.getAllShortestPaths(enums.Labels.USER, enums.RelationshipTypes.IS_FRIEND_OF);
//		ShortestPathAnalysis SPAnalysis = new ShortestPathAnalysis(graphDB);
//		SPAnalysis.getShortestPath(enums.Labels.ACTOR, "Forest Whitaker", enums.Labels.ACTOR, "Miles Teller");
//		SPAnalysis.getAllShortestPaths(enums.Labels.SINGLE_NODE,enums.RelationshipTypes.IS_CONNECTED);
//		PageRankAnalysis PRAnalysis= new PageRankAnalysis(graphDB);
//		
//		PRAnalysis.listDBfunctions();
//		PRAnalysis.setProperties();
//		PRAnalysis.getPageRank();

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
		String createSubGraphMovieDB = "CALL gds.graph.create( " + "  'SUBGRAPH', \n" + // temporary graph name
				"  'ACTOR', \n" + // Nodelabel
				"  'ACTED_WITH')"; // Relation
		
//		@SuppressWarnings("unused")
//		String createSubGraphTextProcessing = "CALL gds.graph.create( " + "  'SUBGRAPH', \n" + // temporary graph name
//				"  'SINGLE_NODE', \n" + // Nodelabel
//				"  'IS_CONNECTED')"; // Relation
		
//		@SuppressWarnings("unused")
//		String createSubGraphDeezer = "CALL gds.graph.create( " + "  'SUBGRAPH', \n" + // temporary graph name
//				"  'USER', \n" + // Nodelabel
//				"  'IS_FRIEND_OF')"; // Relation
		
		@SuppressWarnings("unused")
		String createGraphALL = "CALL gds.graph.create( 'SUBGRAPH_ALL', '*', '*') ";

//		@SuppressWarnings("unused")
//		String createGraphByCypher = "CALL gds.graph.create.cypher('SUBGRAPH','MATCH (n) RETURN id(n) AS id','MATCH (n)-[e]-(m) RETURN id(n) AS source, e.weight AS weight, id(m) AS target, type(e) as type')";
//				

		// @SuppressWarnings("unused")
//		String createGraphByCypher = "CALL gds.graph.create.cypher( " +
//				"  'SUBGRAPH', \n"+ // temporary graph name
//				"  'MATCH (n) RETURN id(n) AS id', \n" +  // Nodelabel
//				"  'MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target, type(r) as type')";  // Relation 

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
		@SuppressWarnings("unused")
		String pageRank = "CALL gds.pageRank.stream('SUBGRAPH', { maxIterations: 100 })\n" + "YIELD nodeId, score \n"
				+ "RETURN gds.util.asNode(nodeId).name AS name, score\n" + "ORDER BY score DESC " + "LIMIT 25";
		
		@SuppressWarnings("unused")
		String pageRankAll = "CALL gds.pageRank.stream('SUBGRAPH_ALL', { maxIterations: 100 })\n" + "YIELD nodeId, score \n"
				+ "RETURN gds.util.asNode(nodeId).name AS name, score\n" + "ORDER BY score DESC " + "LIMIT 25";
		
		@SuppressWarnings("unused")
		String pageRankWeighted = "CALL gds.pageRank.stream('SUBGRAPH') YIELD nodeId, score AS pageRank\n" + "WITH gds.util.asNode(nodeId) AS n, pageRank\n"
				+ "MATCH (n)-[i:IS_CONNECTED]-()\n" + "RETURN n.name AS name, pageRank, count(i) AS degree, sum(i.count) AS weightedDegree\n"
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
//
////		ExEngine.runQuery(createGraphByCypher, true, false);
//		ExEngine.runQuery(createSubGraphMovieDB, true, false);
//		ExEngine.runQuery(pageRank, true, false);


//
////		ExEngine.runQuery(betweenness, true, false);
//		ExEngine.runQuery(createGraphALL, true, false);
//		ExEngine.runQuery(pageRankAll, true, false);
		
//		ExEngine.runQuery(pageRankWeighted, true, false);
////		ExEngine.runQuery(get_herr, true, true);
////		ExEngine.runQuery(hits, true, false);
//
//		ExEngine.exportDBtoFile(outputFile, true, false);
//
////		String endNode = "bums";
////		String startNode = "bums";
////		ExEngine.runShortestPathByCypher(startNode, endNode, true);
//		

		managementService.shutdown();

//		managementServiceDeezer.shutdown();
		System.out.println("SHUTTING DOWN AFTER " + (System.currentTimeMillis() - startTime) + "ms.");
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