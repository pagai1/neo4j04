package mainPackage;

import algoPackage.*;

import java.lang.System;
import java.nio.file.*;
import java.util.Map;
import enums.*;
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
	private static Boolean doAlgo = true;
	private static Boolean mainVerbose = true;
	private static Boolean algoVerbose = false;
	private static Boolean doExport = false;

	// ########################################################
//	// MOVIEDB
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/owndb01/").toPath();
////	private static final File inputFile = new File("/home/pagai/graph-data/tmdb.csv");
//	private static final File inputFile = new File("/home/pagai/graph-data/tmdb_fixed.csv");
//	private static String identifier = "movie";
//	private static enums.Labels mainLabel = enums.Labels.PERSON;
//	private static enums.RelationshipTypes mainRelation = enums.RelationshipTypes.ACTED_WITH;
//	private static int maxRounds = 10001;

//	EDGELIST
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/deezerdb/").toPath();
////	private static final File inputFile = new File("/home/pagai/_studium/_BA/_KN/graph-data/deezer_clean_data/both.csv");
//	private static final File inputFile = new File("/home/pagai/graph-data/pokec/soc-pokec-relationships_weighted.txt");
//	private static String identifier = "deezer";
//	private static Labels mainLabel = Labels.USER;
//	private static RelationshipTypes mainRelation = RelationshipTypes.IS_FRIEND_OF;
//	private static int maxRounds = 10001;

//	GEO
	private static final Path databaseDirectory = new File("/home/pagai/graph-data/OSRM/").toPath();
//	private static final File inputFile = new File("/home/pagai/_studium/_BA/_KN/graph-data/deezer_clean_data/both.csv");
	private static final File inputFile = new File("/home/pagai/graph-data/OSRM/final_semicolon.txt");
	private static String identifier = "geo";
	private static Labels mainLabel = Labels.PLZ;
	private static RelationshipTypes mainRelation = RelationshipTypes.HAS_ROAD_TO;
	private static int maxRounds = 100001;

	// COOCCSDB
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/cooccsdatabase/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/cooccs.csv");
//	private static String identifier = "cooccs";
//	private static enums.Labels mainLabel = enums.Labels.SINGLE_NODE;
//	private static enums.RelationshipTypes mainRelation = enums.RelationshipTypes.IS_CONNECTED;

//	
	// COOCCSDB_EXTERNAL
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/cooccsdatabase/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/cooccs.csv");
//	private static String identifier = "cooccs";
//	private static enums.Labels mainLabel = enums.Labels.SINGLE_NODE;
//	private static enums.RelationshipTypes mainRelation = enums.RelationshipTypes.IS_CONNECTED;
//	private static int maxRounds = 10000;

//	// GENERAL TESTS
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/general_tests/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/general_tests.csv");
//	private static String identifier = "general_tests";
//	private static enums.Labels mainLabel = Labels.SINGLE_NODE;
//	private static enums.RelationshipTypes mainRelation = RelationshipTypes.IS_CONNECTED;
//	private static int maxRounds = 1000001;

	// ########################################################

	/**
	 * Setting config. Is used when opening the database and makes - export of
	 * graph-data to file possible - allows running of GDS algorithms and
	 * apoc-Algorithms via Cypher
	 **/

	private static Map<String, String> config = MapUtil.stringMap("dbms.tx_log.rotation.retention_policy", "500M size",
			"dbms.tx_log.rotation.retention_policy", "2 files");

//	private static Map<String, String> config = MapUtil.stringMap("dbms.security.procedures.unrestricted", "gds.*","dbms.security.procedures.whitelist", "gds.*");
//	private static Map<String, String> config = MapUtil.stringMap("dbms.security.procedures.unrestricted", "gds.*,apoc.*", "dbms.security.procedures.whitelist", "gds.*,apoc.*");
//	private static Map<String, String> config = MapUtil.stringMap("apoc.export.file.enabled", "true");
//	private static Map<String, String> config = MapUtil.stringMap("apoc.export.file.enabled", "true", "dbms.security.procedures.unrestricted",
//			"gds.*,apoc.*", "dbms.security.procedures.whitelist", "gds.*,apoc.*", "dbms.logs.query.time_logging_enabled", "true",
//			"dbms.logs.debug.level", "DEBUG", "dbms.tx_log.rotation.retention_policy", "500M size", "dbms.tx_log.rotation.retention_policy",
//			"2 files");

	private static GraphDatabaseService graphDB;
	private static DatabaseManagementService managementService;

	@SuppressWarnings("unused")
	private static ExecutionEngine ExEngine;
	@SuppressWarnings("unused")
	private static String outputFile = identifier + "db.csv";

	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		System.out.print("BUILDING DATABASE... " + databaseDirectory + "\n");
		long buildTime = System.currentTimeMillis();
		managementService = new DatabaseManagementServiceBuilder(databaseDirectory).setConfigRaw(config).build();
		System.out.println("DONE IN " + (System.currentTimeMillis() - buildTime) + "ms.");
		graphDB = managementService.database("neo4j");
		registerShutdownHook(managementService);
		dataController myDataController = new dataController(graphDB);

//		rounds is here taken to use increasing amount of data and make loops to keep it running to test different sizes. Used for example in general tests.
		for (int round = 0; round < 11; round = round + 111) {
			if (mainVerbose)
				System.out.println("######## STARTING WITH ROUND: " + round);
				System.out.print("######## STARTING ");

			@SuppressWarnings("unused")
			int lineLimit = 0;
			Boolean createIndizes = true;
			if (cleanAndCreate) {
				if (mainVerbose) {
					if (createIndizes) {
						System.out.println("WITH INDIZES #########");
					} else {
						System.out.println("WITHOUT INDIZES #########");
					}
				}
				Boolean clearAndCreateIndizesVerbose = false;
				myDataController.clearDB(graphDB, clearAndCreateIndizesVerbose, 0);
				myDataController.clearIndexes(graphDB, clearAndCreateIndizesVerbose);
//				myDataController.clearDBByCypher(graphDB, clearAndCreateIndizesVerbose);
				if (createIndizes)
					myDataController.createIndexes(graphDB, identifier, clearAndCreateIndizesVerbose);

				long startTime2 = System.currentTimeMillis();

				if (identifier.equals("movie")) {
					myDataController.loadDataFromCSVFile(inputFile, ",", graphDB, false, round, true);
//					myDataController.printAll(graphDB);
				}

				if (identifier.equals("deezer")) {
					myDataController.runDeezerImportByMethods(inputFile, identifier, ",", round, true, true, 0, false);
//					myDataController.runDeezerImportByCypher(inputFile, 10000, true, true, 0);
//					myDataController.printAll(graphDB);
				}

				if (identifier.equals("cooccs")) {
					myDataController.runCooccsImportByMethods(graphDB, inputFile, 0, true);
				}

				if (identifier.equals("geo")) {
					myDataController.runGeoImportByMethods(inputFile, identifier, "|", 0, true, true, 0, true);
				}

				if (identifier.equals("general_tests")) {
//					myDataController.clearDB(graphDB, clearAndCreateIndizesVerbose, 0);
//					myDataController.createIndexes(graphDB, identifier, clearAndCreateIndizesVerbose);
//					myDataController.createNodes(round, mainLabel, false);
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
			} else {
				if (mainVerbose) {
					System.out.println("- JUST OPENING THE DB #########");
					myDataController.printAll(graphDB, true);
				}
			}

			if (doAlgo || doExport) {
				if (mainVerbose)
					System.out.println("######### STARTING ALGO OR EXPORT STUFF #########");
				ExEngine = new ExecutionEngine(graphDB);

				/**
				 * Find Nodes by Degree
				 * 
				 */
//				int degree = 6;
//				myDataController.findNodesDegrees(degree, null, null, null, DegreeFunctions.EQ, false);
//				System.out.println("######## END WITH LINES: " + rounds);

				/**
				 * SHORTEST PATH
				 */

				ShortestPathAnalysis SPAnalysis = new ShortestPathAnalysis(graphDB);
//				SPAnalysis.getAllShortestPaths(mainLabel, mainRelation, "regular" , algoVerbose);
//				SPAnalysis.getAllShortestPaths(mainLabel, mainRelation, "dijkstra" , algoVerbose);
				SPAnalysis.getAllShortestPaths(mainLabel, mainRelation, "astar", algoVerbose);

//				SPAnalysis.getShortestPath(enums.Labels.USER, "5", enums.Labels.USER, "134", enums.RelationshipTypes.IS_FRIEND_OF);
//				SPAnalysis.getShortestPath(enums.Labels.ACTOR, "Forest Whitaker", enums.Labels.ACTOR, "Miles Teller");
//				SPAnalysis.getAllShortestPaths(enums.Labels.SINGLE_NODE,enums.RelationshipTypes.IS_CONNECTED);

				/**
				 * PAGERANK
				 */
//				PageRankAnalysis PRAnalysis= new PageRankAnalysis(graphDB);
//				PRAnalysis.listDBfunctions();
//				PRAnalysis.setProperties();
//				PRAnalysis.getPageRank();

				/**
				 * DEGREE CENTRALITY
				 */

//				DegreeCentralityAnalysis DCAnalysis= new DegreeCentralityAnalysis(graphDB);
//				DCAnalysis.getDegreeCentrality(graphDB,true);

				/**
				 * CYPHERS
				 */
//				@SuppressWarnings("unused")
//				String query = "CALL apoc.export.csv.all(\"" + outputFile + "\", {})";
//				@SuppressWarnings("unused")
//				String query = "CALL gds.list();";
//				@SuppressWarnings("unused")
//				String query = "CALL apoc.help(\"apoc\");";

//				String call_schema = "CALL db.schema()";

				/**
				 * The following Strings create and delete Subgraphs for GDS-Algorithmtests
				 */

//				@SuppressWarnings("unused")
//				String createGraph = "CALL gds.graph.create( " +
//					"  'SUBGRAPH', \n"+ // temporary graph name
//					"  'SINGLE_NODE', \n" +  // Nodelabel
//					"  'IS_CONNECTED', \n" +  // Relation 
//					"  {relationshipProperties: 'cost'})\n"	+ 
//					"YIELD graphName, nodeCount, relationshipCount;\n";
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
				String createSubGraphEdgelist = "CALL gds.graph.create('SUBGRAPH', \n" + // temporary graph name
						"  'USER', \n" + // Nodelabel
						"  'IS_FRIEND_OF')"; // Relation

				@SuppressWarnings("unused")
				String createSubGraphEdgelistReverseOriented = "CALL gds.graph.create('SUBGRAPH', \n" + // temporary graph name
						"  'USER', \n" + // Nodelabel
						"  'IS_FRIEND_OF')"; // Relation

				@SuppressWarnings("unused")
				String createSubGraphEdgelistWithWeight = "CALL gds.graph.create('SUBGRAPH', \n" + // temporary graph name
						"  'USER', \n" + // Nodelabel
						"  { IS_FRIEND_OF: {orientation: 'REVERSE', relationshipWeightProperty: 'weight' })"; // Relation

				@SuppressWarnings("unused")
				String createGraphALL = "CALL gds.graph.create( 'SUBGRAPH', '*', '*') ";
				//
//				@SuppressWarnings("unused")
//				String createGraphByCypher = "CALL gds.graph.create.cypher('SUBGRAPH','MATCH (n) RETURN id(n) AS id','MATCH (n)-[e]-(m) RETURN id(n) AS source, e.weight AS weight, id(m) AS target, type(e) as type')";
//					

				@SuppressWarnings("unused")
				String createFullGraphByCypher = "CALL gds.graph.create.cypher( " + "  'SUBGRAPH', \n" + // temporary graph name
						"  'MATCH (n) RETURN id(n) AS id', \n" + // Nodelabel
						"  'MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target, type(r) as type')"; // Relation

				@SuppressWarnings("unused")
				String removeSubgraphByCypher = "CALL gds.graph.drop('SUBGRAPH')";

				/**
				 * CALL OF ALGORITHMS ON GRAPHS/SUBGRAPHS
				 */

//				@SuppressWarnings("unused")
//				String betweenness = "CALL gds.betweenness.stream(\n" + 
//					" 'SUBGRAPH'\n" + 
//					")\n" + 
//					"YIELD\n" + 
//					"  nodeId,\n" + 
//					"  score \n" +
//					"RETURN gds.util.asNode(nodeId).name AS Name, score\n"+
//					"ORDER BY score ASC;";				

//				@SuppressWarnings("unused")
//				String allShortestPaths = "CALL gds.alpha.allShortestPaths.stream(\n"
//					+ "{nodeProjection: 'USER',\n"
//					+ " relationshipProjection: {\n"
//					+ "		IS_FRIEND_OF: {\n"
//					+ "			type: 'IS_FRIEND_OF',\n"
//					+ "			properties: 'weight'\n"
//					+ "		}\n"
//					+ "	},\n"
//					+ "relationshipWeightProperty: 'weight'})\n"
//					+ "YIELD sourceNodeId, targetNodeId, distance\n"
//					+ "WITH gds.util.asNode(sourceNodeId) AS sourceNode, gds.util.asNode(targetNodeId) AS targetNode, distance AS value\n"
//					+ "RETURN sourceNode.name AS source, targetNode.name AS target, value\n" + 
//					"ORDER BY value DESC, source ASC, target ASC\n" + 
//					"LIMIT 10";

				@SuppressWarnings("unused")
				String allShortestPaths = "CALL gds.alpha.allShortestPaths.stream(\n" + "{nodeProjection: 'USER',\n" + " relationshipProjection: {\n"
						+ "		IS_FRIEND_OF: {\n" + "			type: 'IS_FRIEND_OF',\n" + "			properties: 'weight'\n" + "		}\n"
						+ "	},\n" + "relationshipWeightProperty: 'weight'})\n" + "YIELD sourceNodeId, targetNodeId, distance\n"
						+ "WITH sourceNodeId, targetNodeId, distance \n"
//					+ "WHERE gds.util.isFinite(distance) = true \n"
						+ "MATCH (source:USER) WHERE id(source) = sourceNodeId \n" + "MATCH (target:USER) WHERE id(target) = targetNodeId \n"
						+ "WITH source, target, distance WHERE source <> target \n"
						+ "RETURN source.name AS source, target.name AS target, distance\n";
//					+ "ORDER BY distance ASC, source ASC, target ASC\n"; 
//					+ "LIMIT 10";

				@SuppressWarnings("unused")
				String simRank = "CALL gds.nodeSimilarity.stream('SUBGRAPH' ) " + "YIELD node1, node2, similarity \n"
						+ "RETURN gds.util.asNode(node1).name AS n1, gds.util.asNode(node2).name as n2, similarity \n"
						+ "ORDER BY similarity DESCENDING, n1, n2";

				@SuppressWarnings("unused")
				String pageRank = "CALL gds.pageRank.stream('SUBGRAPH', { maxIterations: '100' })\n" + "YIELD nodeId, score \n"
						+ "RETURN gds.util.asNode(nodeId).name AS name, score\n" + "ORDER BY score DESC " + "LIMIT 25";

				@SuppressWarnings("unused")
				String pageRankAll = "CALL gds.pageRank.stream('SUBGRAPH', { maxIterations: 100 })\n" + "YIELD nodeId, score \n"
						+ "RETURN gds.util.asNode(nodeId).name AS name, score\n" + "ORDER BY score DESC " + "LIMIT 25";

				@SuppressWarnings("unused")
				String degreeCentrality = "CALL gds.alpha.degree.stream(\n" + " 'SUBGRAPH')\n" + "YIELD \n" + "  nodeId,score \n"
						+ "RETURN gds.util.asNode(nodeId).name AS name, score AS followers \n" + "ORDER BY followers DESC, name DESC";

				@SuppressWarnings("unused")
				String pageRankWeighted = "CALL gds.pageRank.stream('SUBGRAPH') YIELD nodeId, score AS pageRank\n"
						+ "WITH gds.util.asNode(nodeId) AS n, pageRank\n" + "MATCH (n)-[i:IS_CONNECTED]-()\n"
						+ "RETURN n.name AS name, pageRank, count(i) AS degree, sum(i.count) AS weightedDegree\n"
						+ "ORDER BY weightedDegree DESC LIMIT 25";

				@SuppressWarnings("unused")
				String hits = "CALL gds.alpha.hits.stream('SUBGRAPH', {hitsIterations: 100}) \n" + "YIELD nodeId,values\n"
						+ "RETURN gds.util.asNode(nodeId).name AS Name, values.auth AS auth, values.hub as hub \n" + "ORDER BY hub DESC";

				@SuppressWarnings("unused")
				String betweennessCentrality = "CALL gds.betweenness.stream(\n" + "  graphName: 'SUBGRAPH',\n" + "  configuration: 'SUBGRAPH'\n"
						+ ")\n" + "YIELD nodeId,score";

				@SuppressWarnings("unused")
				String get_herr = "MATCH (n:SINGLE_NODE)-[rel:IS_CONNECTED]->(m:SINGLE_NODE) RETURN m.name, count(n)";

				@SuppressWarnings("unused")
				String JACCARD = "MATCH (n) \n" + "RETURN gds.alpha.similarity.jaccard([186,123], [123,2732]) AS similarity";
				////

				/**
				 * CYPHER EXECUTION
				 * 
				 */
////			ExEngine.runQuery(createGraphByCypher, true, false);
//				String returnAllNodes = "MATCH (n) RETURN count(*)";
//				ExEngine.runQuery(returnAllNodes, true, false);
//				String returnAllEdges = "MATCH ()-[r]->() RETURN count(r)";
//				ExEngine.runQuery(returnAllEdges, true, false);
				//
////			ExEngine.runQuery(call_schema, true, false);

				/**
				 * SUBGRAPHCREATION
				 */
//				ExEngine.runQuery(createFullGraphByCypher, true, true, " ");
//				ExEngine.runQuery(createSubGraphMovieDB, true, false, " ");
//				ExEngine.runQuery(createSubGraphEdgelist, false, true, "");
//				ExEngine.runQuery(createSubGraphEdgelistReverseOriented, true, false, "");
//				ExEngine.runQuery(createGraphALL, false ,false, "");

				// show that the graph is created
//				ExEngine.runQuery("CALL gds.graph.list()", true, true, "");
				/**
				 * ALGOS
				 */
//				ExEngine.runQuery(allShortestPaths, true, false, " ");
//				ExEngine.runQuery(pageRank, true, true);

				//
////			ExEngine.runQuery(betweenness, true, false);
//				ExEngine.runQuery(pageRankAll, true, false);

//				ExEngine.runQuery(pageRankWeighted, true, false);
////			ExEngine.runQuery(get_herr, true, true);
//				ExEngine.runQuery(hits, true, false, " ");
//				ExEngine.runQuery(simRank, true, false, "|");
//				ExEngine.runQuery(degreeCentrality, false, false, "|");

//				ExEngine.runQuery(JACCARD, true, false, "|");
				if (doExport)
					ExEngine.exportDBtoFile(outputFile, true, false, "");
				//
////			String endNode = "bums";
////			String startNode = "bums";
////			ExEngine.runShortestPathByCypher(startNode, endNode, true);
//				ExEngine.runQuery(removeSubgraphByCypher, false, false, " ");
			}
		}

		managementService.shutdown();

//		managementServiceDeezer.shutdown();
		if (mainVerbose)
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