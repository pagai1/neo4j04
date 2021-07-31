package algoPackage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import enums.Labels;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

public class ShortestPathAnalysis {

	private static GraphDatabaseService graphDB;
	private static long startTime;
	private static double startTimeSingle;
	private static long fullStartTime;

	// Constructor
	public ShortestPathAnalysis(GraphDatabaseService inputGraphDB) {
		graphDB = inputGraphDB;
	}

	public void getShortestPath(Label node1Label, String node1Name, Label node2Label, String node2Name, RelationshipType relType) {
		try (Transaction tx = graphDB.beginTx()) {
			Node startNode = null;
			Node endNode = null;
			ResourceIterable<Node> nodeList = tx.getAllNodes();

			for (Node tmpNode : nodeList) {
				if (tmpNode.getProperty("name").equals(node1Name)) {
					System.out.println("FOUND STARTNODE");
					startNode = tmpNode;
				}
				if (tmpNode.getProperty("name").equals(node2Name)) {
					System.out.println("FOUND ENDNODE");
					endNode = tmpNode;
				}
			}

			// Node startNode = tx.findNode(node1Label, "name", node1Name);
//			Node startNode = tx.findNode(enums.Labels.USER, "name", "5");
//			Node endNode = tx.findNode(node2Label, "name", node2Name);
//			Node endNode = tx.findNode(enums.Labels.USER, "name", "134");

//			System.out.println("SEARCHING SHORTEST PATH FOR: " + startNode.getProperty("name") + " AND "
//					+ endNode.getProperty("name"));
			long startTimeAlgo = System.currentTimeMillis();
			PathFinder<Path> finderShortestPath = GraphAlgoFactory.shortestPath(new BasicEvaluationContext(tx, graphDB),
					PathExpanders.forTypeAndDirection(relType, Direction.BOTH), 100);

			GraphAlgoFactory.dijkstra(new BasicEvaluationContext(tx, graphDB), PathExpanders.forTypeAndDirection(relType, Direction.BOTH), "bums");
			System.out.println("EXECUTED SHORTESTPATH IN " + (System.currentTimeMillis() - startTimeAlgo) + " ms.");

			Path singleShortestPath = finderShortestPath.findSinglePath(startNode, endNode);
			print_path(singleShortestPath, startNode, endNode);
		}
	}

	/**
	 * Method gets all shortest paths between all nodes
	 * 
	 * @param label            - Which label shall have the nodes?
	 * @param relationShipType - which relationship type shall be used in
	 *                         pathfinding?
	 * @param method           - Which algorithm-method shall be taken "regular" or
	 *                         "dijkstra" or "astar" possible
	 * @param verbose          - give output with informations about paths
	 */
	public void getAllShortestPaths(Labels label, enums.RelationshipTypes relationShipType, String method, boolean verbose) {
		fullStartTime = System.currentTimeMillis();
		if (verbose)
			System.out.println("SHORTEST PATH - " + method + " - CREATING PATHFINDERS...");
		try (Transaction tx = graphDB.beginTx()) {
			long edgeCount = tx.getAllRelationships().stream().count();
			List<Node> nodeList = new ArrayList<Node>();

			Iterator<Node> fullNodeListIterator = tx.findNodes(label).stream().iterator();
			while (fullNodeListIterator.hasNext()) {
				Node nodeFromFullList = fullNodeListIterator.next();
				nodeList.add(nodeFromFullList);
			}
			int nodeCount = nodeList.size();
			if (verbose)
				System.out.println("FOUND " + nodeCount + " NODES.");

			if (method == "regular") {
				startTime = System.currentTimeMillis();
				PathFinder<Path> finderShortestPath = GraphAlgoFactory.shortestPath(new BasicEvaluationContext(tx, graphDB),
						PathExpanders.forTypeAndDirection(relationShipType, Direction.BOTH), 50);
				for (int i = 0; i < nodeCount; i++) {
					Node startNode = nodeList.get(i);
					for (int j = 0; j < nodeCount; j++) {
						Node endNode = nodeList.get(j);
						if (endNode != startNode) {
							executeFinderShortestPath(startNode, endNode, finderShortestPath, verbose);
						}
					}
				}
			}

			if (method == "dijkstra") {
				startTime = System.currentTimeMillis();
				PathFinder<WeightedPath> finderDijkstra = GraphAlgoFactory.dijkstra(new BasicEvaluationContext(tx, graphDB),
						PathExpanders.forTypeAndDirection(relationShipType, Direction.BOTH), "weight");
				for (int i = 0; i < nodeCount; i++) {
					Node startNode = nodeList.get(i);
					for (int j = 0; j < nodeCount; j++) {
						Node endNode = nodeList.get(j);
						if (endNode != startNode) {
							executeFinderDijkstra(startNode, endNode, finderDijkstra, verbose);
						}
					}
				}
			}

			if (method == "astar") {
				startTime = System.currentTimeMillis();
				// Estimateevaluator picks distance between nodes in
				// Is multiplied by 10000 to have a better weight in calculation. x.y < xxxx.y
				EstimateEvaluator<Double> estimateEvaluator = new EstimateEvaluator<Double>() {
					@Override
					public Double getCost(final Node node, final Node goal) {
						double dx = Math.abs(Double.valueOf((String) node.getProperty("x")) - Double.valueOf((String) goal.getProperty("x")));
						double dy = Math.abs(Double.valueOf((String) node.getProperty("y")) - Double.valueOf((String) goal.getProperty("y")));
						double result = (Math.sqrt(Math.pow(dx, 2) + Math.pow(dy, 2)) * 10000);
						return result;
					}
				};

				PathFinder<WeightedPath> finderAStar = GraphAlgoFactory.aStar(new BasicEvaluationContext(tx, graphDB),
						PathExpanders.forTypeAndDirection(relationShipType, Direction.OUTGOING), CommonEvaluators.doubleCostEvaluator("weight"),
						estimateEvaluator);
				for (int i = 0; i < nodeCount; i++) {
					Node startNode = nodeList.get(i);
					for (int j = 0; j < nodeCount; j++) {
						Node endNode = nodeList.get(j);
						if (endNode != startNode) {
							executeFinderAStar(startNode, endNode, finderAStar, verbose);
						}
					}
				}
			}

			// shortestPath - Returns an algorithm which can find all shortest paths (that
			// is paths with as short Path.length() as possible) between two nodes.

			// Returns an algorithm which can find all shortest paths (that is paths with as
			// short Path.length() as possible) between two nodes.

			System.out.println("ALL SHORTEST PATHS FOR " + nodeCount + " NODES AND " + edgeCount + " EDGES ENDED IN: "
					+ (System.currentTimeMillis() - startTime) + "ms.");

		}
		if (verbose)
			System.out.println("FULL RUNTIME: " + (System.currentTimeMillis() - fullStartTime) + "ms.");
	}

	/**
	 * Finds the shortest path using default aStar (A*) finder.
	 * 
	 * @param startNode          startnode of path
	 * @param endNode            targetnode of path
	 * @param finderShortestPath the finder-instance
	 * @param verbose            if true, method prints all paths with
	 *                           calculationtime for each path
	 */
	public void executeFinderAStar(Node startNode, Node endNode, PathFinder<WeightedPath> finderAStar, boolean verbose) {
		startTimeSingle = System.nanoTime();
		Path singleShortestPath = finderAStar.findSinglePath(startNode, endNode);
		if (verbose) {
			print_path(singleShortestPath, startNode, endNode);
			System.out.printf("%.9f s.\n", (double) ((System.nanoTime() - startTimeSingle) / 1000000000.0));
		}
//
//		if (singleShortestPath != null) {
//			System.out.print("### shortestPath ### FOUND SHORTESTPATH IN " + (System.currentTimeMillis() - startTime)
//					+ " ms. +++ ");
//			for (Node nodeOnPath : singleShortestPath.nodes()) {
//				System.out.print(nodeOnPath.getProperty("username") + " - ");
//			}
//		} else {
//			System.out.print("NO PATH - ");
//		}

	}

	/**
	 * Finds the shortest path using default shortestPath finder.
	 * 
	 * @param startNode
	 * @param endNode
	 * @param finderShortestPath
	 */
	public void executeFinderShortestPath(Node startNode, Node endNode, PathFinder<Path> finderShortestPath, boolean verbose) {
		startTimeSingle = System.nanoTime();
		Path singleShortestPath = finderShortestPath.findSinglePath(startNode, endNode);
		if (verbose) {
			print_path(singleShortestPath, startNode, endNode);
			System.out.printf("%.9f s.\n", (double) ((System.nanoTime() - startTimeSingle) / 1000000000.0));
		}
//
//		if (singleShortestPath != null) {
//			System.out.print("### shortestPath ### FOUND SHORTESTPATH IN " + (System.currentTimeMillis() - startTime)
//					+ " ms. +++ ");
//			for (Node nodeOnPath : singleShortestPath.nodes()) {
//				System.out.print(nodeOnPath.getProperty("username") + " - ");
//			}
//		} else {
//			System.out.print("NO PATH - ");
//		}

	}

	/**
	 * Prints out all nodes of the path in the order.
	 * 
	 * @param singleShortestPath
	 * @param startNode
	 * @param endNode
	 */
	private void print_path(Path singleShortestPath, Node startNode, Node endNode) {
		if (singleShortestPath == null) {
			System.out.println("No path between " + startNode.getProperty("name") + " and " + endNode.getProperty("name"));
		} else {
			for (Node nodeOnPath : singleShortestPath.nodes()) {
				System.out.print(nodeOnPath.getProperty("name") + " - ");
			}
//			System.out.println("");
		}
	}

	/**
	 * Finds the shortest path using default dijkstraFinder.
	 * 
	 * @param startNode      - the startnode of the path
	 * @param endNode        - the endnode of the path
	 * @param finderDijkstra - the finderalgorithm
	 */
	public void executeFinderDijkstra(Node startNode, Node endNode, PathFinder<WeightedPath> finderDijkstra, Boolean verbose) {
		startTimeSingle = System.nanoTime();
		Path singlePathDijkstra = finderDijkstra.findSinglePath(startNode, endNode);
		if (verbose) {
			print_path(singlePathDijkstra, startNode, endNode);
			System.out.printf("%.9f s.\n", (double) ((System.nanoTime() - startTimeSingle) / 1000000000.0));
		}
	}

	public void findShortestPathByCypher(String nodeName1, String nodeName2) {

		String query = "MATCH (start:user {name: '" + nodeName1 + "'}), (end:user {name: '" + nodeName2 + "'})\n" + ""
				+ "CALL gds.beta.shortestPath.dijkstra.stream({\n" + "      nodeProjection: '*',\n" + "  relationshipProjection: {\n" + "    all: {\n"
				+ "      type: 'IS_FRIEND_OF',\n" + "      orientation: 'UNDIRECTED'\n" + "    }\n" + "  },\n" + "  sourceNode: id(start),\n"
				+ "  targetNode: id(end) })\n" + "YIELD nodeIds,sourceNode,targetNode,totalCost,index\n" + "RETURN\n" + "    index,\n"
				+ "    gds.util.asNode(sourceNode).name AS sourceNodeName,\n" + "    gds.util.asNode(targetNode).name AS targetNodeName,\n"
				+ "    totalCost,\n" + "    [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames\n" + "	 ORDER BY index";
		try (Transaction tx = graphDB.beginTx()) {
			// QUERY 1
			long startTimeQuery = System.currentTimeMillis();
			Result result = tx.execute(query);
			System.out.println("TOOK: " + (System.currentTimeMillis() - startTimeQuery) + "ms.");
			long startTimeClose = System.currentTimeMillis();
			tx.close();
			System.out.println("CLOSE TOOK: " + (System.currentTimeMillis() - startTimeClose) + "ms.");
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				for (Entry<String, Object> column : row.entrySet()) {
					System.out.println(column.getKey() + ": " + column.getValue());
//	                rows += column.getKey() + ": " + column.getValue() + "; ";
				}
//	            rows += "\n";
			}
//			System.out.println(rows);
			System.out.println("QUERY FINISHED.");
		}
	}
}
