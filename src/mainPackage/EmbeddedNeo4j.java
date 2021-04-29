/*
 * Licensed to Neo4j under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo4j licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package mainPackage;

import algoPackage.*;

import java.lang.System;
import java.nio.file.*;
import java.util.Map;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.io.IOException;

import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;

import org.neo4j.graphdb.GraphDatabaseService;

import dataPackage.dataController;

//import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class EmbeddedNeo4j {


	/**
	 * - FOR APOC, DBMS STUFF USE SETCONFIG WITH STRING MAP CONFIG. IT IS ENOUGH.
	 * - plugins-folder needed with apropriate jar-files.
	 * - neo4j.conf file not necessary
	 */
	
	private static final String databaseConfig = "/home/pagai/graph-data/general_db_data/conf/neo4j.conf";
//	private static final File inputFolder = new File("/home/pagai/graph-data/");
//	private static final File importFolder = new File("/var/lib/neo4j/import/");
//	private static final File pluginsFolder = new File("/home/pagai/graph-data/general_db_data/plugins");

	private static final Boolean cleanAndCreate = false;

	//	########################################################
	// MOVIEDB
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/owndb01/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/tmdb.csv");
//	private static String identifier = "movie";

	// DEEZERDB
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/deezerdb/").toPath();
//	private static final File inputFile = new File("/home/pagai/_studium/_BA/_KN/graph-data/deezer_clean_data/RO_edges_2000.csv");
//	private static String identifier = "deezer";

	// COOCCSDB
	private static final Path databaseDirectory = new File("/home/pagai/graph-data/cooccsdatabase/").toPath();
	private static final File inputFile = new File("/home/pagai/graph-data/cooccs.csv");
	private static String identifier = "cooccs";
//	
//	// COOCCSDB_EXTERNAL
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/cooccsdatabase/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/cooccs.csv");
//	private static String identifier = "cooccs";

//	// GENERAL TESTS
//	private static final Path databaseDirectory = new File("/home/pagai/graph-data/general_tests/").toPath();
//	private static final File inputFile = new File("/home/pagai/graph-data/general_tests.csv");
//	private static String identifier = "general_tests";
	
	//	########################################################

	// Setting config.
	// Is used when opening the database and makes
	// - export of graph-data to file possible
	// - allows running of GDS algorithms and apoc-Algorithms via Cypher
//	private static Map<String, String> config = MapUtil.stringMap("apoc.export.file.enabled", "true");
	private static Map<String, String> config = MapUtil.stringMap("apoc.export.file.enabled", "true", "dbms.security.procedures.unrestricted", "gds.*,apoc.*", "dbms.security.procedures.whitelist", "gds.*,apoc.*");

	private static String outputFile = identifier + "db.csv";
	private static GraphDatabaseService graphDB;
	private static DatabaseManagementService managementService;

	private static ExecutionEngine ExEngine;

	public static void main(final String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		System.out.print("BUILDING DATABASE...");
		long buildTime = System.currentTimeMillis();

// 		################ GENERAL TESTS ################
//		
//		managementService = new DatabaseManagementServiceBuilder(databaseDirectory).setConfigRaw(config).build();
//		
//		System.out.println("DONE IN " + (System.currentTimeMillis() - buildTime) + "ms.");
//		System.out.println("USING DATABASE: " + databaseDirectory);
//		graphDB = managementService.database("neo4j");
//		
//		registerShutdownHook(managementService);
//		dataController myDataController = new dataController(graphDB);
//		
//		int amount = 10;
//		myDataController.clearDB(graphDB);
////		myDataController.clearIndexes(graphDB);
//////		myDataController.createIndexes(graphDB, identifier);
//		myDataController.createNodes(amount);
//		myDataController.makeCompleteGraph();
////		myDataController.printAll(graphDB);
		
// 		################ DEEZER DATABASE ################
//		
//		managementService = new DatabaseManagementServiceBuilder(databaseDirectory).setConfigRaw(config).build();
//		
//		System.out.println("DONE IN " + (System.currentTimeMillis() - buildTime) + "ms.");
//		System.out.println("USING DATABASE: " + databaseDirectory_deezer);
//		graphDB = managementService.database("neo4j");
//		
//		registerShutdownHook(managementService);
//		dataController myDataController = new dataController(graphDB);
//		
//		
//		myDataController.runDeezerImportByMethods(inputFile);
////		myDataController.runDeezerImportByCypher(inputFile);
		// myDataController.printAll(graphDB);

//		################ MOVIEDATABASE ################
//		managementServiceMovies = new DatabaseManagementServiceBuilder(databaseDirectory_movies).setConfigRaw(config).build();
////		managementServiceMovies = new DatabaseManagementServiceBuilder(databaseDirectory_movies).setConfigRaw(config).loadPropertiesFromFile(databaseConfig).build();
//
//		System.out.println("DONE IN " + (System.currentTimeMillis() - buildTime) + "ms.");
//		System.out.println("USING DATABASE: " + databaseDirectory_movies);
//		graphDB_movies = managementServiceMovies.database("neo4j");
//
//		if (cleanAndCreate) {
//			dataController myDataController = new dataController(graphDB);
//			myDataController.clearDB(graphDB);
//			myDataController.clearIndexes(graphDB);
//			myDataController.createIndexes(graphDB, identifier);
//			long startTime2 = System.currentTimeMillis();
//			myDataController.loadDataFromCSVFile(inputFile, ",", graphDB, false, 2000);
//			System.out.println("FINISHED LOADING AFTER " + (System.currentTimeMillis() - startTime2) + "ms.");
//		}
//		################ COOCCS DATABASE ################
//		managementService = new DatabaseManagementServiceBuilder(databaseDirectory).build();
//		managementService = new DatabaseManagementServiceBuilder(databaseDirectory).loadPropertiesFromFile(databaseConfig).build();
		managementService = new DatabaseManagementServiceBuilder(databaseDirectory).setConfigRaw(config).build();

		System.out.println("DONE IN " + (System.currentTimeMillis() - buildTime) + "ms.");
		System.out.println("USING DATABASE: " + databaseDirectory);
		graphDB = managementService.database(DEFAULT_DATABASE_NAME);
		dataController myDataController = new dataController(graphDB);

		if (cleanAndCreate) {
			myDataController.clearDB(graphDB);
			myDataController.clearIndexes(graphDB);
			myDataController.createIndexes(graphDB, identifier);
			long startTime2 = System.currentTimeMillis();

			if (identifier.equals("movie")) {
				myDataController.loadDataFromCSVFile(inputFile, ",", graphDB, false, 2000);
			}

			if (identifier.equals("deezer")) {
				myDataController.runDeezerImportByMethods(inputFile);
//				myDataController.runDeezerImportByCypher(inputFile);
			}

			if (identifier.equals("cooccs")) {
				myDataController.runCooccsImportByMethods(inputFile);
			}
			System.out.println("FINISHED IMPORT AFTER " + (System.currentTimeMillis() - startTime2) + "ms.");
		}
//		myMovieDataController.printAll(graphDB_movies);
//		
//		ShortestPathAnalysis SPAnalysis = new ShortestPathAnalysis(graphDB_deezer);
//		SPAnalysis.getShortestPath(enums.Labels.USER, "5", enums.Labels.USER, "134", enums.RelationshipTypes.IS_FRIEND_OF);
//		SPAnalysis.getAllShortestPaths(enums.Labels.USER, enums.RelationshipTypes.IS_FRIEND_OF);
//		ShortestPathAnalysis SPAnalysis = new ShortestPathAnalysis(graphDB_movies);
//		SPAnalysis.getShortestPath(enums.Labels.ACTOR, "Forest Whitaker", enums.Labels.ACTOR, "Miles Teller");
//		SPAnalysis.getAllShortestPaths(enums.Labels.ACTOR);
//		PageRankAnalysis PRAnalysis= new PageRankAnalysis(graphDB_movies);
//		
//		PRAnalysis.listDBfunctions();
//		PRAnalysis.setProperties();
//		PRAnalysis.getPageRank();

//		String query = "CALL apoc.export.csv.all(\"" + outputFile + "\", {})";
//		String query = "CALL gds.list();";
//		String query = "CALL apoc.help(\"apoc\");";

		@SuppressWarnings("unused")
		String createGraph = "CALL gds.graph.create( " +
				"  'WORD_GRAPH', \n"+ // temporary graph name
				"  'SINGLE_NODE', \n" +  // Nodelabel
				"  'IS_CONNECTED' \n" +  // Relation 
				")\n"	+ 
				"YIELD graphName, nodeCount, relationshipCount;\n";

		@SuppressWarnings("unused")
		String createGraphByCypher = "CALL gds.graph.create.cypher('WORD_GRAPH','MATCH (n) RETURN id(n) AS id','MATCH (n)-[e]-(m) RETURN id(n) AS source, e.weight AS weight, id(m) AS target')";
				
		@SuppressWarnings("unused")
		String betweenness = "CALL gds.betweenness.stream(\n" + 
				" 'WORD_GRAPH'\n" + 
				")\n" + 
				"YIELD\n" + 
				"  nodeId,\n" + 
				"  score \n" +
				"RETURN gds.util.asNode(nodeId).name AS Name, score\n"+
				"ORDER BY score ASC;";				

		@SuppressWarnings("unused")
		String hits = "CALL gds.alpha.hits.stream('WORD_GRAPH', {hitsIterations: 100}) \n" +
				"YIELD nodeId,values\n" +
				"RETURN gds.util.asNode(nodeId).name AS Name, values.auth AS auth, values.hub as hub \n" +
				"ORDER BY hub ASC";		


		@SuppressWarnings("unused")
		String returnAllNodes = "MATCH (n) RETURN count(*)";
		
		
		ExEngine = new ExecutionEngine(graphDB);
		ExEngine.runQuery(createGraphByCypher, true, false);
		ExEngine.runQuery(betweenness, true, false);
//		ExEngine.runQuery(hits, true, false);

//		ExEngine.exportDBtoFile(outputFile, true, false);

//		String endNode = "bums";
//		String startNode = "bums";
//		ExEngine.runShortestPathByCypher(startNode, endNode, true);
		managementService.shutdown();

//		managementServiceDeezer.shutdown();
		System.out.println("SHUTTING DOWN AFTER " + (System.currentTimeMillis() - startTime) + "ms.");
	}

	@SuppressWarnings("unused")
	private static void registerShutdownHook(final DatabaseManagementService managementService) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				managementService.shutdown();
			}
		});
	}
}