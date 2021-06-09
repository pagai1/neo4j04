
package dataPackage;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import java.util.Iterator;
import java.util.List;
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

import enums.Labels;

public class dataController {

	private static long startTime;

	public static GraphDatabaseService graphDB;
	private static BufferedReader reader;
	private static BufferedReader reader2;

	public static int lineCountLimit = 0;
	public static HashMap personMap = new HashMap();
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
	 * @param myDeezerDataController
	 */
	public void runDeezerImportByCypher(File inputFile_deezer, int limit) {
//		clearDB(graphDB);
//		clearIndexes(graphDB);
//		createIndexesDeezerDB(graphDB_deezer);
//		createIndexesDeezerDBByCypher(graphDB);
		System.out.println("LOADING BY CYPHER");
//		loadEdgeListbyCypher(graphDB, inputFile_deezer, ',');
//		loadEdgeListbyCypherNodesAndRelations(graphDB, inputFile_deezer, ',');
		loadEdgeListbyCypherInOne(graphDB, inputFile_deezer, ',');

	}

	/**
	 * Loads given number (limit) of lines from edgeList.
	 * 
	 * @param inputFile_deezer path to file
	 * @param limit            number of lines to be loaded from inputfile
	 */
	public void runDeezerImportByMethods(File inputFile, int limit) {
		System.out.println("LOADING DEEZER EDGELIST BY METHODS");
		loadEdgeListbyMethods(graphDB, inputFile, ',', limit, "deezer");
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
	 * @param delimiterm   - The delimiter character.
	 * @param inputgraphDb - The database to load the data in.
	 * @param clearGraph   - If true the graph will be cleared before inserting.
	 * @param limit        - max-linenumber. "all" if "0".
	 */
	public void loadDataFromCSVFile(File inputFile, String delimiterm, GraphDatabaseService inputgraphDb, boolean clearGraph, int limit) {

		try {
			if (limit == 0) {
				lineCountLimit = getNumberOfLines(inputFile) - 1;
			} else {
				lineCountLimit = limit;
//				linecount = 100;	
			}

			System.out.println("LOADING " + lineCountLimit + " ENTRIES FROM FILE " + inputFile.getAbsolutePath());
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
				clearDB(graphDB, true);
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
	 * @param inputgraphDb - the graphdatabase
	 * @param verbose      - give output
	 */
	public void clearDB(GraphDatabaseService inputgraphDb, Boolean verbose) {
		try (Transaction tx = inputgraphDb.beginTx()) {
			if (verbose)
				System.out.print("REMOVING ALL RELATIONSHIPS...");
			int relCount = 0;
			startTime = System.currentTimeMillis();
			Iterable<Relationship> allRelationships = tx.getAllRelationships();
			for (Relationship relationship : allRelationships) {
				relCount = relCount + 1;
				relationship.delete();
			}
			if (verbose)
				System.out.println("TOOK " + (System.currentTimeMillis() - startTime) + " ms.");
			if (verbose)
				System.out.println("COMMITTING DELETION OF " + relCount + " RELATIONS.");
//			startTime = System.currentTimeMillis();
			tx.commit();
			if (verbose)
				System.out.println("TOOK " + (System.currentTimeMillis() - startTime) + "ms.");
		}
		try (Transaction tx = inputgraphDb.beginTx()) {
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
			tx.commit();
			if (verbose)
				System.out.println("TOOK " + (System.currentTimeMillis() - startTime) + "ms.");
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
		List<String> roleList = new ArrayList<>();

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
							if (!full_person_list.contains(actor)) {
								full_actor_list.add(actor);
								full_person_list.add(actor);
//								System.out.println("ADDED ACTOR: " + actor);
							}
//							if (!checkIfExists("name", actor, enums.Labels.ACTOR)) {
//								if (!full_actor_list.contains(actor.toString()) == false ) {
//									full_actor_list.add(actor);
//								}
////								Node actorNode = tx.createNode(enums.Labels.ACTOR);
////								actorNode.setProperty("name", actor);
//							}
						}
					}
					if (headers[i].equals("director")) {
						for (String director : movieline[i].split("\\|")) {
							if (!full_person_list.contains(director)) {
								full_director_list.add(director);
								full_person_list.add(director);
							}
//							if (!checkIfExists("name", director, enums.Labels.DIRECTOR)) {
//								if (!full_director_list.contains(director.toString())) {
//									full_director_list.add(director);
//								}
////								Node directorNode = tx.createNode(enums.Labels.DIRECTOR);
////								directorNode.setProperty("name", director);
//							}
						}
					}
					if (headers[i].equals("production_companies")) {
						for (String company : movieline[i].split("\\|")) {
							if (!full_company_list.contains(company)) {
								full_company_list.add(company);
							}

//							Node companyNode = tx.createNode(enums.Labels.PRODUCTION_COMPANY);
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
			}
			roleList.clear();
			for (String value : full_person_list) {
				if (full_actor_list.contains(value)) {
					personMap.put("ACTOR", true);
				}
				if (full_director_list.contains(value)) {
					personMap.put("DIRECTOR", true);
				}
//				personMap.put("roles", roleList);
				if (!checkIfExists("name", value, enums.Labels.PERSON)) {
					addSingleNode(tx, enums.Labels.PERSON, "name", value, personMap);
				} else {
					System.out.println("NODE: " + value + " ALREADY IN GRAPH");
				}
			}
//			for (String value : full_actor_list) {
//				if (!checkIfExists("name", value, enums.Labels.ACTOR)) {
//					addSingleNode(tx, enums.Labels.ACTOR, "name", value);
//				} else {
//					System.out.println("NODE: " + value + " ALREADY IN GRAPH");
//				}
//			}
//			for (String value : full_director_list) {
//				if (!checkIfExists("name", value, enums.Labels.DIRECTOR)) {
//					addSingleNode(tx, enums.Labels.DIRECTOR, "name", value);
//				} else {
//					System.out.println("NODE: " + value + " ALREADY IN GRAPH");
//				}
//			}
			for (String value : full_company_list) {
				if (!checkIfExists("name", value, enums.Labels.PRODUCTION_COMPANY)) {
					addSingleNode(tx, enums.Labels.PRODUCTION_COMPANY, "name", value, null);
				} else {
					System.out.println("NODE: " + value + " ALREADY IN GRAPH");
				}
			}
			for (String value : full_keyword_list) {
				if (!checkIfExists("name", value, enums.Labels.KEYWORD)) {
					addSingleNode(tx, enums.Labels.KEYWORD, "name", value, null);
				} else {
					System.out.println("NODE: " + value + " ALREADY IN GRAPH");
				}
			}
			for (String value : full_genre_list) {
				if (!checkIfExists("name", value, enums.Labels.GENRE)) {
					addSingleNode(tx, enums.Labels.GENRE, "name", value, null);
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
			List<String> full_director_list, List<String> full_company_list, List<String> full_genre_list, List<String> full_keyword_list, List<String> full_person_list)
			throws IOException {
		String[] movieline = headers;
		int count2 = 0;

		try (Transaction tx = graphDB.beginTx()) {
			while ((movieline != null) & count2 < (lineCountLimit - 1)) {
				movieline = reader.readLine().split((",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
				count2++;
//				System.out.println("MOVIELINE2: " + count2);
				Node movieNode = tx.createNode(enums.Labels.MOVIE);
				for (int i = 0; i < headers.length; i++) {
					if (headers[i].equals("original_title")) {
						movieNode.setProperty("name", movieline[i]);
						movieNode.setProperty("original_title", movieline[i]);
					}


//				}
//				for (int i = 0; i < headers.length; i++) 
//					{
					if (movieline[i].equals(" ")) {
						movieNode.setProperty(headers[i], "unknown");
					} else {
						movieNode.setProperty(headers[i], movieline[i]);
						if (headers[i].equals("cast")) {
							for (String actor : movieline[i].split("\\|")) {
								Relationship relationshipMovie = (tx.findNode(enums.Labels.PERSON, "name", actor)).createRelationshipTo(movieNode,
										enums.RelationshipTypes.ACTED_IN);
								for (String actor2 : movieline[i].split("\\|")) {
									if (!actor.equals(actor2)) {
//										System.out.println("ADDING RELATION BETWEEN: " + actor + " AND " + actor2);
										Relationship relationship = (tx.findNode(enums.Labels.PERSON, "name", actor)).createRelationshipTo(
												tx.findNode(enums.Labels.PERSON, "name", actor2), enums.RelationshipTypes.ACTED_WITH);
										relationship.setProperty("count", 1);
//										Relationship relationship2 = (tx.findNode(enums.Labels.ACTOR, "name", actor2)).createRelationshipTo(
//												tx.findNode(enums.Labels.ACTOR, "name", actor), enums.RelationshipTypes.ACTED_WITH);
//										relationship2.setProperty("count", 1);
									}
								}
							}
						}
						if (headers[i].equals("director")) {
							for (String director : movieline[i].split("\\|")) {
//								System.out.println("DIRECTOR: " + director);
								Relationship relationship3 = (tx.findNode(enums.Labels.PERSON, "name", director)).createRelationshipTo(movieNode,
										enums.RelationshipTypes.DIRECTED);
								relationship3.setProperty("count", 1);
							}
						}
						if (headers[i].equals("keywords")) {
							for (String keyword : movieline[i].split("\\|")) {
								Relationship relationship4 = (movieNode).createRelationshipTo(tx.findNode(enums.Labels.KEYWORD, "name", keyword),
										enums.RelationshipTypes.HAS_KEYWORD);
								relationship4.setProperty("count", 1);

							}
						}
						if (headers[i].equals("genres")) {
							for (String genre : movieline[i].split("\\|")) {
								Relationship relationship5 = (movieNode).createRelationshipTo(tx.findNode(enums.Labels.GENRE, "name", genre),
										enums.RelationshipTypes.IN_GENRE);
								relationship5.setProperty("count", 1);

							}
						}
						if (headers[i].equals("production_companies")) {
							for (String company : movieline[i].split("\\|")) {
								Relationship relationship6 = (tx.findNode(enums.Labels.PRODUCTION_COMPANY, "name", company))
										.createRelationshipTo(movieNode, enums.RelationshipTypes.PRODUCED);
								relationship6.setProperty("count", 1);

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
	 * @param tx2
	 * @param label
	 * @param nodeName
	 */
	private void addSingleNode(Transaction tx2, enums.Labels label, String nameProperty, String nodeName, HashMap properties) {
//		System.out.println("ADDING " + label.toString() + " " + nodeName);
		Node node = tx2.createNode(label);
		node.setProperty(nameProperty, nodeName);
		if (properties != null) {
			Iterator propertyIterator = properties.entrySet().iterator();
			while (propertyIterator.hasNext()) {
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
	private void addPropertyToNode(Transaction tx2, enums.Labels label, String nodeName, String propertyName, String propertyValue) {
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
	private boolean checkIfExists(String property, String name, enums.Labels inputLabel) {
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
	 * Prints out all Nodes with their names and labels
	 * 
	 * @param inputgraphDb
	 */
	public void printAll(GraphDatabaseService inputgraphDb) {
		try (Transaction tx = inputgraphDb.beginTx()) {
			ResourceIterable<Node> nodelist = tx.getAllNodes();
			Iterator<Node> nodeIterator = nodelist.iterator();
			int nodeCount = 0;
			while (nodeIterator.hasNext()) {
				Node nodeFromList = nodeIterator.next();
				String nodeLabels = nodeFromList.getLabels().toString();
				System.out.println("NODE ## NAME: " + nodeFromList.getProperty("name") + " # LABELS: " + nodeLabels);
				nodeCount++;
			}
			ResourceIterable<Relationship> edgelist = tx.getAllRelationships();
			Iterator<Relationship> edgeIterator = edgelist.iterator();
			int edgeCount = 0;
			while (edgeIterator.hasNext()) {
				Relationship edgeFromList = edgeIterator.next();
				System.out.println(
						"EDGE ## FROM: " + edgeFromList.getStartNode().getProperty("name") + " TO: " + edgeFromList.getEndNode().getProperty("name"));
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
			createIndexesMovieDB(graphDB);
		}
		if (identifier.equals("general_tests")) {
			createIndexesGeneralTests(graphDB);
		}
	}

	private void createIndexesGeneralTests(GraphDatabaseService graphDB) {
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			String queryindex = "CREATE INDEX name IF NOT EXISTS FOR (u:node) ON (u.name);";
			tx.execute(queryindex);
			tx.commit();
		}
	}

	public void createIndexesDeezerDBByCypher(GraphDatabaseService graphDB) {
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			String queryindex = "CREATE INDEX name IF NOT EXISTS FOR (u:user) ON (u.name);";
			tx.execute(queryindex);
			tx.commit();
		}
	}

	/**
	 * This function creates the index of userlist for deezer-db.
	 * 
	 * @param graphDB
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
//			schema.constraintFor(enums.Labels.USER).assertPropertyIsUnique("name").withName("usernames").create();
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
//			schema.constraintFor(enums.Labels.USER).assertPropertyIsUnique("name").withName("usernames").create();
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
	public void createIndexesMovieDB(GraphDatabaseService graphDB) {
		IndexDefinition actorNamesIndex, movieNameIndex, keywordIndex, genreIndex, directorIndex, companyIndex;
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			actorNamesIndex = schema.indexFor(Labels.PERSON).on("name").withName("personnames").create();
			tx.commit();
			System.out.println("CREATED INDEX ON PERSONS IN " + (System.currentTimeMillis() - startTime) + "ms");
		}
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			movieNameIndex = schema.indexFor(Labels.MOVIE).on("name").withName("movienames").create();
			tx.commit();
			System.out.println("CREATED INDEX ON MOVIES IN " + (System.currentTimeMillis() - startTime) + "ms");
		}
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			keywordIndex = schema.indexFor(Labels.KEYWORD).on("name").withName("keywordnames").create();
			tx.commit();
			System.out.println("CREATED INDEX ON KEYWORDS IN " + (System.currentTimeMillis() - startTime) + "ms");
		}
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			Schema schema = tx.schema();
			genreIndex = schema.indexFor(Labels.GENRE).on("name").withName("genrenames").create();
			tx.commit();
			tx.close();
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
			tx.close();
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
			System.out.print("REMOVING INDEXES...");
		startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			for (ConstraintDefinition constraintDefinition : tx.schema().getConstraints()) {
				constraintDefinition.drop();
			}
			for (IndexDefinition index : tx.schema().getIndexes()) {
				index.drop();
			}
			tx.commit();
			tx.close();
		}
		if (verbose)
			System.out.println("took " + (System.currentTimeMillis() - startTime) + "ms.");
	}

	public void loadEdgeListbyCypherNodesAndRelations(GraphDatabaseService graphDB, File inputFile, char delimiter) {
		System.out.print("LOADING CSV-FILE...");
		long startTimeq1 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			String query1 = "LOAD CSV FROM 'file://" + inputFile + "' AS line \n" + "MERGE (user1:user {name: line[0]}) \n"
					+ "MERGE (user2:user {name: line[1]})";
			System.out.print("EXECUTING : \n" + query1);
			tx.execute(query1);

			tx.commit();
		}
		System.out.println("STEP1 TOOK: " + (System.currentTimeMillis() - startTimeq1) + "ms.");
		System.out.println("========================");

		long startTimeq3 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 3
			String query3 = "LOAD CSV FROM 'file://" + inputFile + "' AS line\n" + "MATCH (u1:user {name: line[0]})\n"
					+ "MATCH (u2:user {name: line[1]})\n" + "MERGE (u1)-[:IS_FRIEND_OF]->(u2)\n" + "MERGE (u2)-[:IS_FRIEND_OF]->(u1)\n";
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
	 * @param inputfileDeezer - the input file to be loaded
	 * @param c               - delimiter
	 */
	public void loadEdgeListbyCypherInOne(GraphDatabaseService graphDB, File inputFile, char delimiter) {
		System.out.print("LOADING FULL CSV-FILE...");
		long startTimeq1 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			String query1 = "LOAD CSV FROM 'file://" + inputFile + "' AS line \n" + "MERGE (user1:user {name: line[0]}) \n"
					+ "MERGE (user2:user {name: line[1]}) \n" + "MERGE (user1)-[:IS_FRIEND_OF]->(user2) \n"
					+ "MERGE (user2)-[:IS_FRIEND_OF]->(user1) ";
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
	public void loadEdgeListbyCypher(GraphDatabaseService graphDB, File inputFile, char delimiter) {
		System.out.print("LOADING CSV-FILE...");
		long startTimeq1 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			String query1 = "LOAD CSV FROM 'file://" + inputFile + "' AS line\n" + "MERGE (user1:user {name: line[0]});\n";
			System.out.print("EXECUTING : \n" + query1);
			tx.execute(query1);
			tx.commit();
		}
		System.out.println("STEP1 TOOK: " + (System.currentTimeMillis() - startTimeq1) + "ms.");
		System.out.println("========================");
		long startTimeq2 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 2
			String query2 = "LOAD CSV FROM 'file://" + inputFile + "' AS line\n" + "MERGE (user2:user {name: line[1]});\n";
			System.out.print("EXECUTING : \n" + query2);
			tx.execute(query2);
			tx.commit();
		}
		System.out.println("STEP2 TOOK: " + (System.currentTimeMillis() - startTimeq2) + "ms.");
		System.out.println("========================");

		long startTimeq3 = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 3
			String query3 = "LOAD CSV FROM 'file://" + inputFile + "' AS line\n" + "MATCH (u1:user {name: line[0]})\n"
					+ "MATCH (u2:user {name: line[1]})\n" + "MERGE (u1)-[:IS_FRIEND_OF]->(u2)\n" + "MERGE (u2)-[:IS_FRIEND_OF]->(u1)\n";
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
	 */
	public void loadEdgeListbyMethods(GraphDatabaseService graphDB, File inputFile, char delimiter, int limit, String identifier) {
		enums.Labels currentLabel = null;
		enums.RelationshipTypes currentRelType = null;
		if (identifier.equals("cooccs")) {
			currentLabel = enums.Labels.SINGLE_NODE;
			currentRelType = enums.RelationshipTypes.IS_CONNECTED;
		}
		if (identifier.equals("deezer")) {
			currentLabel = enums.Labels.USER;
			currentRelType = enums.RelationshipTypes.IS_FRIEND_OF;
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
			System.out.println("LOADING " + lineCountLimit + " ENTRIES FROM FILE " + inputFile.getAbsolutePath());
			reader = new BufferedReader(new FileReader(inputFile));
			int count = 0;
			System.out.println("ADDING NODES...");
			long startTime1 = System.currentTimeMillis();

			String nodeLine = reader.readLine();
			while (nodeLine != null & count < (lineCountLimit)) {
				String node1 = nodeLine.split(delimiterString)[0];
				if (!full_node_list.contains(node1)) {
					full_node_list.add(node1);
				}
				String node2 = nodeLine.split(delimiterString)[1];
				if (!full_node_list.contains(node2)) {
					full_node_list.add(node2);
				}
				// go to the next line
				count++;
				nodeLine = reader.readLine();
			}
			int nodeCount = 0;
			try (Transaction tx = graphDB.beginTx()) {
				System.out.println("STARTING TRANSACTION...");
				for (String nodeName : full_node_list) {
					nodeCount++;
					addSingleNode(tx, currentLabel, "name", nodeName, null);
				}

				tx.commit();
				System.out.println("ADDED " + nodeCount + " NODES BY METHOD. " + (System.currentTimeMillis() - startTime1) + "ms.");
			}

			// Loading Lines to create relations
			reader2 = new BufferedReader(new FileReader(inputFile));
			int lineCounter = 0;
			System.out.println("ADDING EDGES...");
			long startTime2 = System.currentTimeMillis();
			try (Transaction tx = graphDB.beginTx()) {
				String edgeLine = reader2.readLine();
				while ((edgeLine != null) & lineCounter < (lineCountLimit)) {
					String nodeName1 = edgeLine.split(",")[0];
					String nodeName2 = edgeLine.split(",")[1];
					Node firstNode = tx.findNode(currentLabel, "name", nodeName1);
					Node secondNode = tx.findNode(currentLabel, "name", nodeName2);
					@SuppressWarnings("unused")
					Relationship relationship1 = firstNode.createRelationshipTo(secondNode, currentRelType);
					@SuppressWarnings("unused")
					Relationship relationship2 = secondNode.createRelationshipTo(firstNode, currentRelType);
					lineCounter++;
					edgeLine = reader2.readLine();
				}

				tx.commit();
				System.out.println("ADDED " + lineCounter + " LINES BY METHOD. " + (System.currentTimeMillis() - startTime2) + "ms.");
			}
			System.out.println("# IMPORT VIA METHOD TOOK: " + (System.currentTimeMillis() - startTime) + "ms.");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void runCooccsImportByMethods(File inputfile, boolean verbose) {
		clearDB(graphDB, true);
		clearIndexes(graphDB, true);
		createIndexes(graphDB, "cooccs", verbose);
		System.out.println("LOADING BY METHODS");
		loadEdgeListbyMethods(graphDB, inputfile, ',', 0, "cooccs");
	}

	/**
	 * This method is just creating nodes
	 * 
	 * @param amount
	 */
	public void createNodes(int amount, Boolean verbose) {
		long startTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			long createStart = System.currentTimeMillis();
			for (int i = 0; i < amount; i++) {
//				Node tmpNode = tx.createNode(enums.Labels.SINGLE_NODE);
//				tx.createNode();
				tx.createNode(enums.Labels.SINGLE_NODE);
//				tmpNode.setProperty("name", i);
			}
			tx.commit();
			long endCreate = System.currentTimeMillis();

			int timeCreate = (int) (endCreate - createStart);
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
		int nodeCounter = 0;
		int i = 0;
		int j = 0;
		long createTime = System.currentTimeMillis();
		ArrayList<Node> nodeList = null;
		try (Transaction tx = graphDB.beginTx()) {
			for (Node tmpNode : tx.getAllNodes()) {
				nodeList.add(tmpNode);
			}
			for (i = 0; i < nodeList.size() - 1; i++) {
				for (j = i + 1; j < nodeList.size() - 1; j++) {
					Node node1 = nodeList.get(i);
					Node node2 = nodeList.get(j);
					if (!node2.getProperties("name").equals(node1.getProperty("name"))) {
						Relationship relationship1 = node1.createRelationshipTo(node2, enums.RelationshipTypes.IS_CONNECTED);
						Relationship relationship2 = node2.createRelationshipTo(node1, enums.RelationshipTypes.IS_CONNECTED);
						relcounter = relcounter + 2;
					}
				}
			}
			tx.commit();
		}
		System.out.println("##### RELATIONSHIP CREATION OF " + relcounter + " RELATIONS TOOK: " + (System.currentTimeMillis() - createTime) + "ms.");
	}

	public long getNumberOfNodes(GraphDatabaseService graphDB2) {
		long nodeCount = 0;
		try (Transaction tx = graphDB.beginTx()) {
			nodeCount = tx.getAllNodes().stream().count();
		}
		return nodeCount;
	}
}
