
package dataPackage;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.lang.System;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import enums.*;

public class dataController {

	private static long startTime;

	public static GraphDatabaseService graphDB;
	private static BufferedReader reader;
	private static BufferedReader reader2;

	public static int lineCountLimit = 0;
	public static HashMap<String, Boolean> personMap = new HashMap<String, Boolean>();
	public static List<String> full_actor_list = new ArrayList<String>();
	public static List<String> full_director_list = new ArrayList<String>();
	public static List<String> full_company_list = new ArrayList<String>();
	public static List<String> full_genre_list = new ArrayList<String>();
	public static List<String> full_keyword_list = new ArrayList<String>();
	public static List<String> full_person_list = new ArrayList<String>();

	public static List<String> full_node_list = new ArrayList<String>();

//	
//	public static List<String> unique_actors = new ArrayList<String>();
//	public static List<String> unique_directors = new ArrayList<String>();
//	public static List<String> unique_companies = new ArrayList<String>();
//	public static List<String> unique_genres = new ArrayList<String>();
//	public static List<String> unique_keywords = new ArrayList<String>();

	// Constructor
	public dataController(GraphDatabaseService inputgraphDb) {
		graphDB = inputgraphDb;
	}

	/**
	 * Loads the file via the uncommented method
	 * 
	 * @param inputFile      (inputfile
	 * @param limit          - max number of edges to be loaded (0=all)
	 * @param weighted       - is it weighted or not
	 * @param periodicCommit - periodic commit after given number of transactions, 0
	 *                       = no periodic commit
	 */
	public void runDeezerImportByCypher(File inputFile, int limit, boolean weighted, boolean directed, int periodicCommit) {
//		clearDB(graphDB);
//		clearIndexes(graphDB, true);
//		createIndexesDeezerDB(graphDB, true);
//		createIndexesDeezerDBByCypher(graphDB);
		System.out.println("LOADING BY " + limit + " LINES BY CYPHER");
//		loadEdgeListbyCypher(graphDB, inputFile_deezer, ',', weighted);
//		loadEdgeListbyCypherNodesAndRelations(graphDB, inputFile_deezer, ',', weighted);
		loadEdgeListbyCypherInOne(graphDB, inputFile, ',', limit, weighted, directed, periodicCommit);

	}

	/**
	 * Loads given number (limit) of lines from edgeList.
	 * 
	 * @param inputFile      path to file
	 * @param delimiter      give a char which is used to separate the columns
	 * @param weighted       if true, third row of csv will be taken as weight-value
	 * @param directed       if false, for each line 2 relations will be created
	 * @param periodicCommit Periodic commit after a given number of transactions.
	 */
	public void runGeoImportByMethods(File inputFile, String identifier, String delimiter, int limit, boolean weighted, boolean directed,
			int periodicCommit, boolean verbose) {
		int count = 0;
		Labels currentLabel = null;
		RelationshipTypes currentRelType = null;
		if (verbose)
			System.out.println("LOADING GEODATA BY METHODS");

		if (identifier.equals("geo")) {
			currentLabel = Labels.PLZ;
			currentRelType = RelationshipTypes.HAS_ROAD_TO;
		}

		if (limit == 0) {
			// get all lines of inputfile
			try {
				lineCountLimit = getNumberOfLines(inputFile);
				System.out.println("LIMIT SET TO: " + lineCountLimit);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			lineCountLimit = limit;
			System.out.println("LIMIT SET TO GIVEN : " + lineCountLimit);
		}

		try {
			reader = new BufferedReader(new FileReader(inputFile));
			String nodeLine = reader.readLine();
			while (nodeLine != null & count < (lineCountLimit)) {
				String node1 = nodeLine.split(";")[0];
				full_node_list.add(node1);
				String node2 = nodeLine.split(";")[5];
				full_node_list.add(node2);
				// go to the next line
				count++;
				nodeLine = reader.readLine();
			}
			reader.close();
			List<String> full_node_list_unique = full_node_list.stream().distinct().collect(Collectors.toList());
			long startTime1 = System.currentTimeMillis();
			try (Transaction tx = graphDB.beginTx()) {
				int nodeCount = 0;
				if (verbose)
					System.out.println("STARTING TRANSACTION...");
				for (String nodeName : full_node_list_unique) {
					nodeCount++;
					addSingleNode(tx, currentLabel, "plz", nodeName, null);
				}
				tx.commit();
				if (verbose)
					System.out.println("ADDED : " + nodeCount + " NODES BY METHOD. " + (System.currentTimeMillis() - startTime1) + "ms.");
			}
			// Loading Lines to create relations
			reader2 = new BufferedReader(new FileReader(inputFile));

			int lineCounter = 0;
			if (verbose)
				System.out.println("ADDING EDGES...");
			long startTime2 = System.currentTimeMillis();
			Transaction tx = graphDB.beginTx();
			try {
				String edgeLine = reader2.readLine();
				while ((edgeLine != null) & lineCounter < (lineCountLimit)) {

					String nodeName1 = edgeLine.split(";")[0];
					String nodeName2 = edgeLine.split(";")[5];
					Node firstNode = tx.findNode(currentLabel, "plz", nodeName1);
					Node secondNode = tx.findNode(currentLabel, "plz", nodeName2);
					firstNode.setProperty("name", edgeLine.split(";")[1]);
					firstNode.setProperty("x", edgeLine.split(";")[2]);
					firstNode.setProperty("y", edgeLine.split(";")[3]);
					secondNode.setProperty("name", edgeLine.split(";")[6]);
					secondNode.setProperty("x", edgeLine.split(";")[7]);
					secondNode.setProperty("y", edgeLine.split(";")[8]);
					@SuppressWarnings("unused")
					Relationship relationship1 = firstNode.createRelationshipTo(secondNode, currentRelType);
					if (weighted) {
						int weight = Integer.parseInt(edgeLine.split(";")[4]);
						relationship1.setProperty("weight", weight);
					}
					if (!directed) {
						@SuppressWarnings("unused")
						Relationship relationship2 = secondNode.createRelationshipTo(firstNode, currentRelType);
						if (weighted) {
							String weight = edgeLine.split(";")[4];
							relationship2.setProperty("weight", weight);
						}
					}
					lineCounter++;
					edgeLine = reader2.readLine();
				}

				if (verbose)
					System.out.println("ADDED : " + lineCounter + " EDGES BY METHOD. " + (System.currentTimeMillis() - startTime2) + "ms.");
			} finally {
				tx.commit();
			}

			reader2.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (verbose)
			System.out.println("# IMPORT VIA METHOD TOOK: " + (System.currentTimeMillis() - startTime) + "ms.");
	}

	/**
	 * Loads given number (limit) of lines from edgeList.
	 * 
	 * @param inputFile      path to file
	 * @param delimiter      give a char which is used to separate the columns
	 * @param limit          number of lines to be loaded from inputfile
	 * @param weighted       if true, third row of csv will be taken as weight-value
	 * @param directed       if false, for each line 2 relations will be created
	 * @param periodicCommit Periodic commit after a given number of transactions.
	 */
	public void runDeezerImportByMethods(File inputFile, String delimiter, String identifier, int limit, boolean weighted, boolean directed,
			int periodicCommit, boolean verbose) {
		if (verbose)
			System.out.println("LOADING EDGELIST BY METHODS");
		loadEdgeListbyMethods(graphDB, inputFile, delimiter, limit, identifier, weighted, directed, periodicCommit, verbose);
	}

	public int getNumberOfLines(File inputFile) throws IOException {
		int noOfLines = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
			while (reader.readLine() != null) {
				noOfLines++;
			}
		}
		return noOfLines;
	}

	/**
	 * Loads data from given CSV file.
	 * 
	 * @param inputFile    - The inputfile to be loaded.
	 * @param delimiter    - The delimiter character.
	 * @param inputgraphDb - The database to load the data in.
	 * @param clearGraph   - If true the graph will be cleared before inserting.
	 * @param limit        - max-linenumber. "all" if "0".
	 * @param verbose      - write out more text.
	 */
	public void loadDataFromCSVFile(File inputFile, String delimiter, GraphDatabaseService inputgraphDb, boolean clearGraph, int limit,
			boolean verbose) {

		try {
			// FINDING OUT THE MAXIMUM POSSIBLE.
			int lineCount = getNumberOfLines(inputFile) - 1;
			if (limit == 0) {
				lineCountLimit = lineCount;
			} else {
				lineCountLimit = Math.min(limit, lineCount);
//				linecount = 100;	
			}

//			if (verbose) System.out.println("LOADING " + lineCountLimit + " ENTRIES FROM FILE " + inputFile.getAbsolutePath());
			String[] headers, headers2;
			startTime = System.currentTimeMillis();
			reader = new BufferedReader(new FileReader(inputFile));
			reader2 = new BufferedReader(new FileReader(inputFile));

			// Reads headers
			// Crazy splitting is needed because of ","(additional comma) within the
			// "overview" column which
			// describes the movie.
			headers = reader.readLine().split((",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
			headers2 = reader2.readLine().split((",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));

//			System.out.println("HEADERS: " + Arrays.toString(headers));

			if (clearGraph) {
				clearDB(graphDB, true, 0);
			}
			readFile(graphDB, reader, headers, full_actor_list, full_director_list, full_company_list, full_genre_list, full_keyword_list,
					full_person_list);

			readFile2(graphDB, reader2, headers2, full_actor_list, full_director_list, full_company_list, full_genre_list, full_keyword_list,
					full_person_list);
			System.out.println("READ " + lineCountLimit + " ENTRIES IN " + (System.currentTimeMillis() - startTime) + " ms.");
//			printAll(graphDB);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Removes all nodes and all relationships within graph.
	 * 
	 * @param graphDB        - the graphdatabase
	 * @param verbose        - give output
	 * @param periodicCommit - commit deletion after given number of transactions
	 * 
	 */
	public void clearDB(GraphDatabaseService graphDB, Boolean verbose, int periodicCommit) {
		Transaction tx = graphDB.beginTx();
		int relCount = 0;
		try {
			if (verbose)
				System.out.println("REMOVING ALL RELATIONSHIPS...");
			startTime = System.currentTimeMillis();
			Iterable<Relationship> allRelationships = tx.getAllRelationships();
			for (Relationship relationship : allRelationships) {
				relCount = relCount + 1;
				relationship.delete();
				if (periodicCommit != 0) {
					if (relCount % periodicCommit == 0) {
						System.out.println("ADDITIONAL PERIODIC COMMIT AFTER " + periodicCommit);
						System.out.println("BUMS: " + relCount);
						tx.commit();
						System.out.println("COMMITTED");
						tx.close();
						System.out.println("CLOSED");
						tx = graphDB.beginTx();
						System.out.println("BEGIN");
					}
				}
			}
			if (verbose)
				System.out.println("TOOK " + (System.currentTimeMillis() - startTime) + " ms.");
			if (verbose)
				System.out.println("COMMITTING DELETION OF " + relCount + " RELATIONS.");
//			startTime = System.currentTimeMillis();
			if (verbose)
				System.out.println("TOOK " + (System.currentTimeMillis() - startTime) + "ms.");
		} finally {
			tx.commit();
		}
		tx = graphDB.beginTx();
		try {
			if (verbose)
				System.out.print("REMOVING NODES...");
			startTime = System.currentTimeMillis();
			ResourceIterable<Node> nodeList = tx.getAllNodes();
			int nodeCount = 0;
			for (Node nodetodelete : nodeList) {
				nodeCount = nodeCount + 1;
				nodetodelete.delete();
			}
			if (verbose)
				System.out.println("REMOVED " + nodeCount + " NODES. THIS TOOK " + (System.currentTimeMillis() - startTime) + " ms.");
			if (verbose)
				System.out.println("COMMITTING DELETION OF " + nodeCount + " NODES.");
//			startTime = System.currentTimeMillis();
			if (verbose)
				System.out.println("TOOK " + (System.currentTimeMillis() - startTime) + "ms.");
		} finally {
			tx.commit();
		}
	}

	/**
	 * This Method loads the file which is read by the reader-instance. It creates
	 * nodes for all of them and adds them to the graph.
	 * 
	 * @param inputgraphDb
	 * @param reader
	 * @param headers
	 * @param full_actor_list
	 * @param full_director_list
	 * @param full_company_list
	 * @param full_genre_list
	 * @param full_keyword_list
	 * @throws IOException
	 */
	private void readFile(GraphDatabaseService inputgraphDb, BufferedReader reader, String[] headers, List<String> full_actor_list,
			List<String> full_director_list, List<String> full_company_list, List<String> full_genre_list, List<String> full_keyword_list,
			List<String> full_person_list) throws IOException {
		String[] movieline = headers;
//		String nextMovieLine;
		List<String> roleList = new ArrayList<>();
//		movieline = reader.readLine().split((",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
		int count = 0;
		try (Transaction tx = graphDB.beginTx()) {
			while ((movieline != null) & count < (lineCountLimit - 1)) {
				movieline = reader.readLine().split((",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
				count++;
//				System.out.println("MOVIELINE1: " + count);
				for (int i = 0; i < headers.length; i++) {
					if (movieline[i].equals(" ")) {
						movieline[i] = "unknown";
					}
					if (headers[i].equals("cast")) {
						for (String actor : movieline[i].split("\\|")) {
							if (!full_actor_list.contains(actor)) {
								full_actor_list.add(actor);
							}
							if (!full_person_list.contains(actor)) {
								full_person_list.add(actor);

							}

//							if (!checkIfExists("name", actor, Labels.ACTOR)) {
//								if (!full_actor_list.contains(actor.toString()) == false ) {
//									full_actor_list.add(actor);
//								}
////								Node actorNode = tx.createNode(Labels.ACTOR);
////								actorNode.setProperty("name", actor);
//							}
						}
					}
					if (headers[i].equals("director")) {
						for (String director : movieline[i].split("\\|")) {
							if (!full_director_list.contains(director)) {
								full_director_list.add(director);
							}
							if (!full_person_list.contains(director)) {
								full_person_list.add(director);
							}

//							if (!checkIfExists("name", director, Labels.DIRECTOR)) {
//								if (!full_director_list.contains(director.toString())) {
//									full_director_list.add(director);
//								}
////								Node directorNode = tx.createNode(Labels.DIRECTOR);
////								directorNode.setProperty("name", director);
//							}
						}
					}
					if (headers[i].equals("production_companies")) {
						for (String company : movieline[i].split("\\|")) {
							if (!full_company_list.contains(company)) {
								full_company_list.add(company);
							}

//							Node companyNode = tx.createNode(Labels.PRODUCTION_COMPANY);
//							companyNode.setProperty("name", company);
						}
					}
					if (headers[i].equals("keywords")) {
						for (String keyword : movieline[i].split("\\|")) {

							if (!full_keyword_list.contains(keyword)) {
								full_keyword_list.add(keyword);
							}
						}
					}
					if (headers[i].equals("genres")) {
						for (String genre : movieline[i].split("\\|")) {
							if (!full_genre_list.contains(genre)) {
								full_genre_list.add(genre);
							}
						}
					}
				}
//				nextMovieLine = reader.readLine();
//				if (nextMovieLine != null) {
//					System.out.println(nextMovieLine);
//					movieline = nextMovieLine.split((",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
//				} else {
//					movieline = null;
//				}

			}
			roleList.clear();
			for (String value : full_person_list) {
				personMap.put("ACTOR", false);
				personMap.put("DIRECTOR", false);
				if (full_actor_list.contains(value)) {
					personMap.put("ACTOR", true);
				}
				if (full_director_list.contains(value)) {
					personMap.put("DIRECTOR", true);
				}
//				System.out.println(personMap);
//				personMap.put("roles", roleList);
				if (!checkIfExists("name", value, Labels.PERSON)) {
					addSingleNode(tx, Labels.PERSON, "name", value, personMap);
				} else {
					System.out.println("NODE: " + value + " ALREADY IN GRAPH");
				}
			}
//			for (String value : full_actor_list) {
//				if (!checkIfExists("name", value, Labels.ACTOR)) {
//					addSingleNode(tx, Labels.ACTOR, "name", value);
//				} else {
//					System.out.println("NODE: " + value + " ALREADY IN GRAPH");
//				}
//			}
//			for (String value : full_director_list) {
//				if (!checkIfExists("name", value, Labels.DIRECTOR)) {
//					addSingleNode(tx, Labels.DIRECTOR, "name", value);
//				} else {
//					System.out.println("NODE: " + value + " ALREADY IN GRAPH");
//				}
//			}
			for (String value : full_company_list) {
				if (!checkIfExists("name", value, Labels.PRODUCTION_COMPANY)) {
					addSingleNode(tx, Labels.PRODUCTION_COMPANY, "name", value, null);
				} else {
					System.out.println("NODE: " + value + " ALREADY IN GRAPH");
				}
			}
			for (String value : full_keyword_list) {
				if (!checkIfExists("name", value, Labels.KEYWORD)) {
					addSingleNode(tx, Labels.KEYWORD, "name", value, null);
				} else {
					System.out.println("NODE: " + value + " ALREADY IN GRAPH");
				}
			}
			for (String value : full_genre_list) {
				if (!checkIfExists("name", value, Labels.GENRE)) {
					addSingleNode(tx, Labels.GENRE, "name", value, null);
				} else {
					System.out.println("NODE: " + value + " ALREADY IN GRAPH");
				}
			}
			tx.commit();
		}
	}

	/**
	 * Second reading of file to create the movienodes and creates the relationship
	 * between all previously created nodes.
	 * 
	 * @param inputgraphDb
	 * @param reader
	 * @param headers
	 * @param full_actor_list
	 * @param full_director_list
	 * @param full_company_list
	 * @param full_genre_list
	 * @param full_keyword_list
	 * @throws IOException
	 */
	private void readFile2(GraphDatabaseService inputgraphDb, BufferedReader reader, String[] headers, List<String> full_actor_list,
			List<String> full_director_list, List<String> full_company_list, List<String> full_genre_list, List<String> full_keyword_list,
			List<String> full_person_list) throws IOException {
		String[] movieline = headers;
		int count2 = 0;

		try (Transaction tx = graphDB.beginTx()) {
			while ((movieline != null) & count2 < (lineCountLimit - 1)) {
				movieline = reader.readLine().split((",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
				count2++;
//				System.out.println("MOVIELINE2: " + count2);
				Node movieNode = tx.createNode(Labels.MOVIE);
				for (int i = 0; i < headers.length; i++) {
					if (headers[i].equals("original_title")) {
						movieNode.setProperty("name", movieline[i]);
						movieNode.setProperty("original_title", movieline[i]);
					}

//				}
//				for (int i = 0; i < headers.length; i++) 
//					{
					if (movieline[i].equals(" ") || movieline[i] == null) {
						movieNode.setProperty(headers[i], "unknown");
					} else {
						movieNode.setProperty(headers[i], movieline[i]);
						if (headers[i].equals("cast")) {
							for (String actor : movieline[i].split("\\|")) {
								@SuppressWarnings("unused")
								Relationship relationshipMovie = (tx.findNode(Labels.PERSON, "name", actor)).createRelationshipTo(movieNode,
										RelationshipTypes.ACTED_IN);
								for (String actor2 : movieline[i].split("\\|")) {
									if (!actor.equals(actor2)) {
//										System.out.println("ADDING RELATION BETWEEN: " + actor + " AND " + actor2);
										Relationship relationship = (tx.findNode(Labels.PERSON, "name", actor))
												.createRelationshipTo(tx.findNode(Labels.PERSON, "name", actor2), RelationshipTypes.ACTED_WITH);
										relationship.setProperty("weight", 1);
//										Relationship relationship2 = (tx.findNode(Labels.ACTOR, "name", actor2)).createRelationshipTo(
//												tx.findNode(Labels.ACTOR, "name", actor), RelationshipTypes.ACTED_WITH);
//										relationship2.setProperty("count", 1);
									}
								}
							}
						}
						if (headers[i].equals("director")) {
							for (String director : movieline[i].split("\\|")) {
//								System.out.println("DIRECTOR: " + director);
								Relationship relationship3 = (tx.findNode(Labels.PERSON, "name", director)).createRelationshipTo(movieNode,
										RelationshipTypes.DIRECTED);
								relationship3.setProperty("weight", 1);
							}
						}
						if (headers[i].equals("keywords")) {
							for (String keyword : movieline[i].split("\\|")) {
								Relationship relationship4 = (movieNode).createRelationshipTo(tx.findNode(Labels.KEYWORD, "name", keyword),
										RelationshipTypes.HAS_KEYWORD);
								relationship4.setProperty("weight", 1);

							}
						}
						if (headers[i].equals("genres")) {
							for (String genre : movieline[i].split("\\|")) {
								Relationship relationship5 = (movieNode).createRelationshipTo(tx.findNode(Labels.GENRE, "name", genre),
										RelationshipTypes.IN_GENRE);
								relationship5.setProperty("weight", 1);

							}
						}
						if (headers[i].equals("production_companies")) {
							for (String company : movieline[i].split("\\|")) {
								Relationship relationship6 = (tx.findNode(Labels.PRODUCTION_COMPANY, "name", company)).createRelationshipTo(movieNode,
										RelationshipTypes.PRODUCED);
								relationship6.setProperty("weight", 1);

							}
						}

					}
				}

			}
			tx.commit();
		}
	}

	/**
	 * Methods adds a single node to the graph with given label and value for
	 * property "name".
	 * 
	 * @param tx
	 * @param label
	 * @param nodeName
	 */
	private void addSingleNode(Transaction tx, enums.Labels label, String nameProperty, String nodeName, HashMap<String, Boolean> properties) {
		Node node = tx.createNode(label);
		node.setProperty(nameProperty, nodeName);
		if (properties != null) {
			Iterator<?> propertyIterator = properties.entrySet().iterator();
			while (propertyIterator.hasNext()) {
				@SuppressWarnings("rawtypes")
				HashMap.Entry propertyEntry = (HashMap.Entry) propertyIterator.next();
				node.setProperty((String) propertyEntry.getKey(), propertyEntry.getValue());
			}
		}
	}

	/**
	 * Adds a property to a node. Not yet used.
	 * 
	 * @param tx2
	 * @param label
	 * @param nodeName
	 * @param propertyName
	 * @param propertyValue
	 */
	@SuppressWarnings("unused")
	private void addPropertyToNode(Transaction tx2, Labels label, String nodeName, String propertyName, String propertyValue) {
		Node node = tx2.findNode(label, propertyName, propertyValue);
		node.setProperty("name", nodeName);
	}

	/**
	 * Can be used to check if a node is already existing in graph
	 * 
	 * @param property
	 * @param name
	 * @param inputLabel
	 * @return
	 */
	private boolean checkIfExists(String property, String name, Labels inputLabel) {
		boolean found = false;
		try (Transaction tx = graphDB.beginTx()) {
//			System.out.println("SEARCHING: " + name + " IN PROPERTY: " + property);
			Node searchNode = tx.findNode(inputLabel, property, name);
			if (searchNode != null) {
				found = true;
//				System.out.println("FOUND: " + searchNode.getProperty("name"));
			}
		}
		return found;
	}

	/**
	 * Prints out informations about the graph
	 * 
	 * @param inputgraphDb - the graphDB to get info about
	 * @param moreDetails  - if true output shorts really all nodes and edges - if
	 *                     false only count of nodes and edges is shown
	 */
	public void printAll(GraphDatabaseService inputgraphDb, Boolean moreDetails) {
		try (Transaction tx = inputgraphDb.beginTx()) {
			ResourceIterable<Node> nodelist = tx.getAllNodes();
			Iterator<Node> nodeIterator = nodelist.iterator();
			int nodeCount = 0;
			while (nodeIterator.hasNext()) {
				Node nodeFromList = nodeIterator.next();
				String nodeLabels = nodeFromList.getLabels().toString();
				if (moreDetails)
					System.out.println("NODE ## NAME: " + nodeFromList.getProperty("name") + " # LABELS: " + nodeLabels);
				nodeCount++;
			}
			ResourceIterable<Relationship> edgelist = tx.getAllRelationships();
			Iterator<Relationship> edgeIterator = edgelist.iterator();
			int edgeCount = 0;
			while (edgeIterator.hasNext()) {
				Relationship edgeFromList = edgeIterator.next();
				if (moreDetails)
					System.out.println("EDGE ## FROM: " + edgeFromList.getStartNode().getProperty("name") + " TO: "
							+ edgeFromList.getEndNode().getProperty("name"));
				edgeCount++;
			}

			System.out.println("NODECOUNT: " + nodeCount);
			System.out.println("EDGECOUNT: " + edgeCount);

		}
	}

	public void createIndexes(GraphDatabaseService graphDB, String identifier, boolean verbose) {
		if (identifier.equals("cooccs")) {
			createIndexesCooccsDB(graphDB, verbose);
		}
		if (identifier.equals("deezer")) {
			createIndexesDeezerDB(graphDB, verbose);
		}
		if (identifier.equals("movie")) {
			createIndexesMovieDB(graphDB, verbose);
		}
		if (identifier.equals("general_tests")) {
//			createIndexesByCypherGeneralTests(graphDB);
			createIndexesGeneralTests(graphDB, verbose);
		}
	}

	private void createIndexesGeneralTests(GraphDatabaseService graphDB, boolean verbose) {
		@SuppressWarnings("unused")
		IndexDefinition userIndex;
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			userIndex = schema.indexFor(Labels.SINGLE_NODE).on("name").withName("nodenames").create();
			tx.commit();
			if (verbose)
				System.out.println("CREATED INDEX ON USERS IN " + (System.currentTimeMillis() - startTime) + "ms");
		} catch (Exception e) {
			System.out.println("There was already an index.");
		}
	}

	/**
	 * Creates an index for general tests Executes "CREATE INDEX name IF NOT EXISTS
	 * FOR (u:SINGLE_NODE) ON (u.name)"
	 * 
	 * @param graphDB - give a database instance
	 */
	@SuppressWarnings("unused")
	private void createIndexesByCypherGeneralTests(GraphDatabaseService graphDB) {
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			String queryindex = "CREATE INDEX name IF NOT EXISTS FOR (u:SINGLE_NODE) ON (u.name);";
			tx.execute(queryindex);
			tx.commit();
		}
	}

	/**
	 * Creates an index for general tests Executes "CREATE INDEX name IF NOT EXISTS
	 * FOR (u:user) ON (u.name);"
	 * 
	 * @param graphDB - give a database instance
	 */
	public void createIndexesDeezerDBByCypher(GraphDatabaseService graphDB) {
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			String queryindex = "CREATE INDEX name IF NOT EXISTS FOR (u:user) ON (u.name);";
			tx.execute(queryindex);
			tx.commit();
		}
	}

	/**
	 * This function creates the index of nodes for cooccs-db.
	 * 
	 * @param graphDB - database instance
	 */
	@SuppressWarnings("unused")
	public void createIndexesCooccsDB(GraphDatabaseService graphDB, boolean verbose) {
		startTime = System.currentTimeMillis();
		IndexDefinition userIndex;
		startTime = System.currentTimeMillis();
		IndexDefinition wordNamesIndex;
		if (verbose)
			System.out.println("CREATING INDEX FOR SINGLE_NODE NAME");

		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			wordNamesIndex = schema.indexFor(Labels.SINGLE_NODE).on("name").withName("wordnames").create();
			tx.commit();
			if (verbose)
				System.out.println("CREATED INDEX ON WORDS IN " + (System.currentTimeMillis() - startTime) + "ms");
		} catch (Exception e) {
			System.out.println("There was already an index.");
		}

//		try (Transaction tx = graphDB.beginTx()) {
//			Schema schema = tx.schema();
//			schema.constraintFor(Labels.USER).assertPropertyIsUnique("name").withName("usernames").create();
//			tx.commit();
//			System.out.println("CREATED INDEX ON USERS IN " + (System.currentTimeMillis() - startTime) + "ms");
//		}

	}

	@SuppressWarnings("unused")
	/**
	 * This function creates the index of userlist for deezer-db.
	 * 
	 * @param graphDB
	 */
	public void createIndexesDeezerDB(GraphDatabaseService graphDB, boolean verbose) {
		startTime = System.currentTimeMillis();
		IndexDefinition userIndex;
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			userIndex = schema.indexFor(Labels.USER).on("name").withName("usernames").create();
			tx.commit();
			if (verbose)
				System.out.println("CREATED INDEX ON USERS IN " + (System.currentTimeMillis() - startTime) + "ms");
		} catch (Exception e) {
			System.out.println("There was already an index.");
		}

//		try (Transaction tx = graphDB.beginTx()) {
//			Schema schema = tx.schema();
//			schema.constraintFor(Labels.USER).assertPropertyIsUnique("name").withName("usernames").create();
//			tx.commit();
//			System.out.println("CREATED INDEX ON USERS IN " + (System.currentTimeMillis() - startTime) + "ms");
//		}
	}

	@SuppressWarnings("unused")
	/**
	 * This method creates indexes for actornames, movienames, keywords, genres,
	 * directory and company-names.
	 * 
	 * @param graphDB
	 */
	/**
	 * @param graphDB
	 */
	public void createIndexesMovieDB(GraphDatabaseService graphDB, Boolean verbose) {
		IndexDefinition actorNamesIndex, movieNameIndex, keywordIndex, genreIndex, directorIndex, companyIndex;
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			actorNamesIndex = schema.indexFor(Labels.PERSON).on("name").withName("personnames").create();
			tx.commit();
			if (verbose)
				System.out.println("CREATED INDEX ON PERSONS IN " + (System.currentTimeMillis() - startTime) + "ms");
		}
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			movieNameIndex = schema.indexFor(Labels.MOVIE).on("name").withName("movienames").create();
			tx.commit();
			if (verbose)
				System.out.println("CREATED INDEX ON MOVIES IN " + (System.currentTimeMillis() - startTime) + "ms");
		}
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			keywordIndex = schema.indexFor(Labels.KEYWORD).on("name").withName("keywordnames").create();
			tx.commit();
			if (verbose)
				System.out.println("CREATED INDEX ON KEYWORDS IN " + (System.currentTimeMillis() - startTime) + "ms");
		}
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			genreIndex = schema.indexFor(Labels.GENRE).on("name").withName("genrenames").create();
			tx.commit();
//			tx.close();
			if (verbose)
				System.out.println("CREATED INDEX ON GENRES IN " + (System.currentTimeMillis() - startTime) + "ms");
		}
//		startTime = System.currentTimeMillis();
//		try (Transaction tx = graphDB.beginTx()) {
//			Schema schema = tx.schema();
//			directorIndex = schema.indexFor(Labels.DIRECTOR).on("name").withName("directornames").create();
//			tx.commit();
//			tx.close();
//			System.out.println("CREATED INDEX ON DIRECTORS IN " + (System.currentTimeMillis() - startTime) + "ms");
//		}
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			companyIndex = schema.indexFor(Labels.PRODUCTION_COMPANY).on("name").withName("companynames").create();
			tx.commit();
//			tx.close();
			if (verbose)
				System.out.println("CREATED INDEX ON COMPANIES IN " + (System.currentTimeMillis() - startTime) + "ms");
		}

	}

	/**
	 * Removes all constraintdefitions and indexes from given DB.
	 * 
	 * @param graphDB - the database instance
	 * @param verbose - print stuff
	 */
	public void clearIndexes(GraphDatabaseService graphDB, Boolean verbose) {
		if (verbose)
			System.out.println("REMOVING INDEXES...");
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			for (ConstraintDefinition constraintDefinition : tx.schema().getConstraints()) {
				constraintDefinition.drop();
			}
			for (IndexDefinition index : tx.schema().getIndexes()) {
				if (verbose)
					System.out.print("\nREMOVING INDEX: " + index.getName());
				index.drop();
			}
			tx.commit();
//			tx.close();
		}
		if (verbose)
			System.out.println("TOOK " + (System.currentTimeMillis() - startTime) + "ms.");
	}

	public void loadEdgeListbyCypherNodesAndRelations(GraphDatabaseService graphDB, File inputFile, char delimiter, boolean weighted) {
		String weightString = "";
		if (weighted) {
			weightString = " {weight: line[2]}";
		}
		System.out.print("LOADING CSV-FILE...");
		long startTimeq1 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			String query1 = "LOAD CSV FROM 'file://" + inputFile + "' AS line \n" + "MERGE (user1:USER {name: line[0]}) \n"
					+ "MERGE (user2:USER {name: line[1]})";
			System.out.print("EXECUTING : \n" + query1);
			tx.execute(query1);

			tx.commit();
		}
		System.out.println("STEP1 TOOK: " + (System.currentTimeMillis() - startTimeq1) + "ms.");
		System.out.println("========================");

		long startTimeq3 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 3
			String query3 = "LOAD CSV FROM 'file://" + inputFile + "' AS line\n" + "MATCH (u1:USER {name: line[0]})\n"
					+ "MATCH (u2:USER {name: line[1]})\n" + "MERGE (u1)-[:IS_FRIEND_OF" + weightString + "]->(u2)\n" + "MERGE (u2)-[:IS_FRIEND_OF"
					+ weightString + "]->(u1)\n";
			System.out.println("EXECUTING : \n" + query3);
			tx.execute(query3);
			tx.commit();
		}
		System.out.println("STEP2 TOOK: " + (System.currentTimeMillis() - startTimeq3) + "ms.");
		System.out.println("========================");
		System.out.println("##### IMPORT VIA CYPHER TOOK: " + (System.currentTimeMillis() - startTime) + "ms.");
	}

	/**
	 * Loads csv-edgelist to database.
	 * 
	 * @param inputFile      - the input file to be loaded
	 * @param delimiter      - delimiter
	 * @param weighted       - load third column for weight-value of edge
	 * @param graphDB        - the DB instance
	 * @param limit          - number of lines to be loaded from given file (0 =
	 *                       all)
	 * @param directed       - if the edgelist contains both connections of nodes,
	 *                       then it is directed, otherwise undirected, then the
	 *                       second connection has to be added. weight will be the
	 *                       same for both ways.
	 * @param periodicCommit - commit for given number of transactions
	 */
	public void loadEdgeListbyCypherInOne(GraphDatabaseService graphDB, File inputFile, char delimiter, int limit, boolean weighted, boolean directed,
			int periodicCommit) {
		String periodicCommitString = "";
		if (periodicCommit != 0) {
			periodicCommitString = "USING PERIODIC COMMIT " + periodicCommit + " ";
		}
		String weightString = "";
		String rowlimitation = "";
		if (limit != 0) {
			rowlimitation = "WITH line LIMIT " + limit + "\n";
		}
		if (weighted) {
			weightString = " {weight: toInteger(line[2])}";
		}
		System.out.print("LOADING FULL CSV-FILE...");
		long startTimeq1 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY
			String query1 = periodicCommitString + "LOAD CSV FROM 'file://" + inputFile + "' AS line \n" + rowlimitation
					+ "MERGE (user1:USER {name: line[0]}) \n" + "MERGE (user2:USER {name: line[1]}) \n" + "MERGE (user1)-[:IS_FRIEND_OF"
					+ weightString + "]->(user2) \n";
			if (!directed) {
				query1 = query1.concat("MERGE (user2)-[:IS_FRIEND_OF" + weightString + "]->(user2) \n");
			}
			System.out.print("EXECUTING : \n" + query1);
			tx.execute(query1);
			tx.commit();
		}
		System.out.println("STEP1 TOOK: " + (System.currentTimeMillis() - startTimeq1) + "ms.");
		System.out.println("========================");
		System.out.println("##### IMPORT VIA CYPHER TOOK: " + (System.currentTimeMillis() - startTime) + "ms.");
	}

	/**
	 * Loads csv-edgelist to database.
	 * 
	 * @param inputfileDeezer - the input file to be loaded
	 * @param c               - delimiter
	 */
	public void loadEdgeListbyCypher(GraphDatabaseService graphDB, File inputFile, char delimiter, boolean weighted) {
		System.out.print("LOADING CSV-FILE...");
		long startTimeq1 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			String query1 = "LOAD CSV FROM 'file://" + inputFile + "' AS line\n" + "MERGE (user1:USER {name: line[0]});\n";
			System.out.print("EXECUTING : \n" + query1);
			tx.execute(query1);
			tx.commit();
		}
		System.out.println("STEP1 TOOK: " + (System.currentTimeMillis() - startTimeq1) + "ms.");
		System.out.println("========================");
		long startTimeq2 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 2
			String query2 = "LOAD CSV FROM 'file://" + inputFile + "' AS line\n" + "MERGE (user2:USER {name: line[1]});\n";
			System.out.print("EXECUTING : \n" + query2);
			tx.execute(query2);
			tx.commit();
		}
		System.out.println("STEP2 TOOK: " + (System.currentTimeMillis() - startTimeq2) + "ms.");
		System.out.println("========================");

		long startTimeq3 = System.currentTimeMillis();
		String weightString = "";
		if (weighted) {
			weightString = " {weight: line[2]}";
		}
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 3
			String query3 = "LOAD CSV FROM 'file://" + inputFile + "' AS line\n" + "MATCH (u1:USER {name: line[0]})\n"
					+ "MATCH (u2:USER {name: line[1]})\n" + "MERGE (u1)-[:IS_FRIEND_OF" + weightString + "]->(u2)\n"
					+ "MERGE (u2)-[:IS_FRIEND_OF]->(u1)\n";
			System.out.print("EXECUTING : \n" + query3);
			tx.execute(query3);
			tx.commit();
		}
		System.out.println("STEP3 TOOK: " + (System.currentTimeMillis() - startTimeq3) + "ms.");
		System.out.println("========================");
		System.out.println("##### IMPORT VIA CYPHER TOOK: " + (System.currentTimeMillis() - startTime) + "ms.");
	}

	/**
	 * Loads edge list by methods. Creates first Node and Second node and their
	 * relation per each line. Hopefully double-nodes will not be created again.
	 * Uniqueness shall be enforced by Schema-Definition.
	 * 
	 * @param graphDB    the graphDB instance to be used
	 * @param inputFile  path to file
	 * @param delimiter  separator between nodes in line of file
	 * @param limit      number of lines to loaded from inputfile
	 * @param identifier can be one of "cooccs" or "deezer"
	 * @param weighted   load third column for weight-value of edge
	 * @param directed   if false (undirected) graph, 2 relations will be created
	 *                   for each line
	 */
	@SuppressWarnings("resource")
	public void loadEdgeListbyMethods(GraphDatabaseService graphDB, File inputFile, String delimiter, int limit, String identifier, boolean weighted,
			boolean directed, int periodicCommit, boolean verbose) {
		Labels currentLabel = null;
		RelationshipTypes currentRelType = null;
		if (identifier.equals("cooccs")) {
			currentLabel = Labels.SINGLE_NODE;
			currentRelType = RelationshipTypes.IS_CONNECTED;
		}
		if (identifier.equals("deezer")) {
			currentLabel = Labels.USER;
			currentRelType = RelationshipTypes.IS_FRIEND_OF;
		}

		String delimiterString = String.valueOf(delimiter);
		try {
			if (limit == 0) {
				// get all lines of inputfile
				lineCountLimit = getNumberOfLines(inputFile);
			} else {
				lineCountLimit = limit;
			}

			// Loading Lines to create nodes
			if (verbose)
				System.out.println("LOADING " + lineCountLimit + " ENTRIES FROM FILE " + inputFile.getAbsolutePath());
			reader = new BufferedReader(new FileReader(inputFile));
			int count = 0;
			if (verbose)
				System.out.println("COLLECTING NODES...");
			long startTime1 = System.currentTimeMillis();

			String nodeLine = reader.readLine();
			while (nodeLine != null & count < (lineCountLimit)) {
				String node1 = nodeLine.split(delimiterString)[0];
				full_node_list.add(node1);
				String node2 = nodeLine.split(delimiterString)[1];
				full_node_list.add(node2);
				// go to the next line
				count++;
//				System.out.print("LOADED : " + count + " LINES OF FILE.\r");
				nodeLine = reader.readLine();
			}
			reader.close();
//			System.out.println("FULL LIST: " + full_node_list.size());
			List<String> full_node_list_unique = full_node_list.stream().distinct().collect(Collectors.toList());
//			System.out.println("FULL UNIQUE LIST: " + full_node_list_unique.size());
			int nodeCount = 0;
			try (Transaction tx = graphDB.beginTx()) {
				if (verbose)
					System.out.println("STARTING TRANSACTION...");
				for (String nodeName : full_node_list_unique) {
					nodeCount++;
					addSingleNode(tx, currentLabel, "name", nodeName, null);
//					System.out.print("ADDED : " + count + " NODES.\r");
				}
				tx.commit();
				if (verbose)
					System.out.println("ADDED " + nodeCount + " NODES BY METHOD. " + (System.currentTimeMillis() - startTime1) + "ms.");
			}
			// Loading Lines to create relations
			reader2 = new BufferedReader(new FileReader(inputFile));

// WORKING WITH MEMALLOC PROB			
//			int lineCounter = 0;
//			System.out.println("ADDING EDGES...");
//			long startTime2 = System.currentTimeMillis();
//			try (Transaction tx = graphDB.beginTx()) {
//				String edgeLine = reader2.readLine();
//				while ((edgeLine != null) & lineCounter < (lineCountLimit)) {
//					String nodeName1 = edgeLine.split(",")[0];
//					String nodeName2 = edgeLine.split(",")[1];
//					Node firstNode = tx.findNode(currentLabel, "name", nodeName1);
//					Node secondNode = tx.findNode(currentLabel, "name", nodeName2);
//					@SuppressWarnings("unused")
//					Relationship relationship1 = firstNode.createRelationshipTo(secondNode, currentRelType);
//					if (weighted) {
//						int weight = Integer.parseInt(edgeLine.split(",")[2]);
//						relationship1.setProperty("weight", weight);
//					}
//					if (!directed) {
//						@SuppressWarnings("unused")
//						Relationship relationship2 = secondNode.createRelationshipTo(firstNode, currentRelType);
//						if (weighted) {
//							String weight = edgeLine.split(",")[2];
//							relationship2.setProperty("weight", weight);
//						}
//					}
//
//					lineCounter++;
//					edgeLine = reader2.readLine();
//				}
//				tx.commit();
//				System.out.println("ADDED " + lineCounter + " LINES BY METHOD. " + (System.currentTimeMillis() - startTime2) + "ms.");
//			}				
//			reader2.close();

			// WORKAROUND OF MEMALLOC PROB

			int lineCounter = 0;
			if (verbose)
				System.out.println("ADDING EDGES...");
			long startTime2 = System.currentTimeMillis();
			Transaction tx = graphDB.beginTx();
			try {
				String edgeLine = reader2.readLine();
				while ((edgeLine != null) & lineCounter < (lineCountLimit)) {
					if (periodicCommit != 0) {
						if (lineCounter % periodicCommit == 0) {
							if (verbose)
								System.out.println("ADDITIONAL PERIODIC COMMIT AFTER " + periodicCommit);
							tx.commit();
//							tx.close();
							tx = graphDB.beginTx();
						}
					}
					String nodeName1 = edgeLine.split(",")[0];
					String nodeName2 = edgeLine.split(",")[1];
					Node firstNode = tx.findNode(currentLabel, "name", nodeName1);
					Node secondNode = tx.findNode(currentLabel, "name", nodeName2);
					@SuppressWarnings("unused")
					Relationship relationship1 = firstNode.createRelationshipTo(secondNode, currentRelType);
					if (weighted) {
						int weight = Integer.parseInt(edgeLine.split(",")[2]);
						relationship1.setProperty("weight", weight);
					}
					if (!directed) {
						@SuppressWarnings("unused")
						Relationship relationship2 = secondNode.createRelationshipTo(firstNode, currentRelType);
						if (weighted) {
							String weight = edgeLine.split(",")[2];
							relationship2.setProperty("weight", weight);
						}
					}

					lineCounter++;
					edgeLine = reader2.readLine();
				}

				if (verbose)
					System.out.println("ADDED " + lineCounter + " LINES BY METHOD. " + (System.currentTimeMillis() - startTime2) + "ms.");

			} finally {
				tx.commit();
			}
			reader2.close();

			if (verbose)
				System.out.println("# IMPORT VIA METHOD TOOK: " + (System.currentTimeMillis() - startTime) + "ms.");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void runCooccsImportByMethods(GraphDatabaseService graphDB, File inputfile, int periodicCommit, boolean verbose) {
//		clearDB(graphDB, true);
//		clearIndexes(graphDB, true);
//		createIndexes(graphDB, "cooccs", verbose);
		if (verbose)
			System.out.println("LOADING COOCCS BY METHODS");
		loadEdgeListbyMethods(graphDB, inputfile, ",", 0, "cooccs", true, true, periodicCommit, verbose);
	}

	/**
	 * This method is just creating a given amount nodes with a given label and
	 * commits to database instance.
	 * 
	 * @param amount    - give the amount of nodes to be created
	 * @param mainLabel - give the initial label for the node to be created
	 * @param verbose   - give more outoput
	 */
	public void createNodes(int amount, Labels mainLabel, Boolean verbose) {
		if (amount == 0)
			amount = 1;
		long startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			long createStart = System.currentTimeMillis();
			for (int i = 0; i < amount; i++) {
				// Node without label, nearly same speed like for both methods.
//				Node tmpNode = tx.createNode();
//				tx.createNode();

				// Node with initial label on creation
//				Node tmpNode = tx.createNode(mainLabel);
//				
				// Node with adding label
//				Node tmpNode = tx.createNode();
//				tmpNode.addLabel(mainLabel);

				// Node with adding property
				Node tmpNode = tx.createNode();
				tmpNode.setProperty("name", i);

				// Node with adding label and property
//				Node tmpNode = tx.createNode();
//				tmpNode.addLabel(mainLabel);
//				tmpNode.setProperty("name", i);
			}

			tx.commit();
			long endCreate = System.currentTimeMillis();
			int timeCreate = (int) (endCreate - createStart);
			if (timeCreate == 0)
				timeCreate = 1;
			System.out.println(amount + "," + (double) (timeCreate / 1000.0) + "," + (double) ((amount * 1000) / timeCreate));
		}
		if (verbose)
			System.out.println("##### NODECREATION OF " + amount + " NODES TOOK: " + (System.currentTimeMillis() - startTime) + "ms.");
	}

	/**
	 * This method gets all nodes and connects them to each other.
	 * 
	 */
	public void makeCompleteGraph() {
		int relcounter = 0;
		int i = 0;
		int j = 0;
		long createTime = System.currentTimeMillis();
		ArrayList<Node> nodeList = new ArrayList<Node>();
		try (Transaction tx = graphDB.beginTx()) {
			for (Node tmpNode : tx.getAllNodes()) {
				nodeList.add(tmpNode);
			}
			for (i = 0; i < nodeList.size() - 1; i++) {
				for (j = i + 1; j < nodeList.size() - 1; j++) {
					Node node1 = nodeList.get(i);
					Node node2 = nodeList.get(j);
					if (!node2.getProperties("name").equals(node1.getProperty("name"))) {
						@SuppressWarnings("unused")
						Relationship relationship1 = node1.createRelationshipTo(node2, RelationshipTypes.IS_CONNECTED);
						@SuppressWarnings("unused")
						Relationship relationship2 = node2.createRelationshipTo(node1, RelationshipTypes.IS_CONNECTED);
						relcounter = relcounter + 2;
					}
				}
			}
			tx.commit();
		}
		System.out.println("##### RELATIONSHIP CREATION OF " + relcounter + " RELATIONS TOOK: " + (System.currentTimeMillis() - createTime) + "ms.");
	}

	/**
	 * Returns the number of nodes in the given graph given by database-instance
	 * 
	 * @param graphDB - the database instance
	 * @return nodeCount - number of nodes
	 */
	public long getNumberOfNodes(GraphDatabaseService graphDB) {
		long nodeCount = 0;
		try (Transaction tx = graphDB.beginTx()) {
			nodeCount = tx.getAllNodes().stream().count();
		}
		return nodeCount;
	}

	public void clearDBByCypher(GraphDatabaseService graphDB, Boolean verbose) {
		System.out.println("DELETING EVERYTHING BY CYPHER");
		long startTimeq1 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			String query1 = "MATCH (n)\nDETACH DELETE n";
			if (verbose)
				System.out.print("EXECUTING : \n" + query1);
			tx.execute(query1);
			tx.commit();
		}
		System.out.println("EXECUTION TOOK " + (System.currentTimeMillis() - startTimeq1) + " ms.");
	}

	/**
	 * Finds nodes for a given degree.
	 * 
	 * @param inputDegree
	 * @param gt
	 * @param verbose
	 */
	public void findNodesDegrees(int inputDegree, Labels label, String propertyName, String propertyValue, DegreeFunctions gt, Boolean verbose) {
		HashMap<String, Integer> nodeList = new HashMap<String, Integer>();
		int numberAllNodes = 0;
		long startTimeq1 = System.currentTimeMillis();
		switch (gt) {
		case GT:
			if (verbose)
				System.out.println("GETTING ALL NODES WITH GIVEN DEGREE GT " + inputDegree);
			try (Transaction tx = graphDB.beginTx()) {
				for (Node node : tx.getAllNodes()) {
					numberAllNodes++;
					if (node.getDegree() > inputDegree) {
						nodeList.put((String) (node.getProperty("name")), node.getDegree());
					}
				}
			}
			break;
		case LT:
			if (verbose)
				System.out.println("GETTING ALL NODES WITH GIVEN DEGREE LT " + inputDegree);
			try (Transaction tx = graphDB.beginTx()) {
				for (Node node : tx.getAllNodes()) {
					numberAllNodes++;
					if (node.getDegree() < inputDegree) {
						nodeList.put((String) (node.getProperty("name")), node.getDegree());
					}
				}
			}
			break;
		case EQ:
			if (verbose)
				System.out.println("GETTING ALL NODES WITH GIVEN DEGREE EQ " + inputDegree);
			try (Transaction tx = graphDB.beginTx()) {
				for (Node node : tx.getAllNodes()) {
					numberAllNodes++;
					if (node.getDegree() == inputDegree) {
						nodeList.put((String) (node.getProperty("name")), node.getDegree());
					}
				}
			}
			break;
		case GE:
			if (verbose)
				System.out.println("GETTING ALL NODES WITH GIVEN DEGREE GE " + inputDegree);
			try (Transaction tx = graphDB.beginTx()) {
				for (Node node : tx.getAllNodes()) {
					numberAllNodes++;
					if (node.getDegree() <= inputDegree) {
						nodeList.put((String) (node.getProperty("name")), node.getDegree());
					}
				}
			}
			break;
		case LE:
			if (verbose)
				System.out.println("GETTING ALL NODES WITH GIVEN DEGREE LE " + inputDegree);
			try (Transaction tx = graphDB.beginTx()) {
				for (Node node : tx.getAllNodes()) {
					numberAllNodes++;
					if (node.getDegree() >= inputDegree) {
						nodeList.put((String) (node.getProperty("name")), node.getDegree());
					}
				}
			}
			break;
		default:
			if (verbose)
				System.out.println("GETTING ALL NODES WITH GIVEN DEGREE EQ " + inputDegree);
			try (Transaction tx = graphDB.beginTx()) {
				for (Node node : tx.getAllNodes()) {
					numberAllNodes++;
					if (node.getDegree() == inputDegree) {
						nodeList.put((String) (node.getProperty("name")), node.getDegree());
					}
				}
			}
			break;
		}

		if (verbose) {
			System.out.println(nodeList);
			System.out.println("NUMBER: " + nodeList.size());
		}

		System.out.println("EXECUTION TOOK " + (System.currentTimeMillis() - startTimeq1) + " ms. " + "FOUND " + nodeList.size() + " NODES OUT OF "
				+ numberAllNodes + ".");
	}
}
