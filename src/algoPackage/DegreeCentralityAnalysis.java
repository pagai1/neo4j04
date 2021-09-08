package algoPackage;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class DegreeCentralityAnalysis {
	private static GraphDatabaseService graphDB;
	private static Map<String, Integer> degreeList = new HashMap<String, Integer>();
	private static long algoStartTime;
	private static long algoEndTime;

	public DegreeCentralityAnalysis(GraphDatabaseService inputGraphDB) {
		graphDB = inputGraphDB;
	}

	/**
	 * Method to get an output for calculation of all the most central nodes sorted by their degree 
	 * @param graphDB - the graphDB instance
	 * @param verbose - if true, the list will be printed out, if false only the runtime will be printed out
	 */
	public void getDegreeCentrality(GraphDatabaseService graphDB, boolean verbose) {
		algoStartTime = System.currentTimeMillis();
		try (Transaction tx = graphDB.beginTx()) {
			for (Node tmpNode : tx.getAllNodes()) {
				degreeList.put((String) tmpNode.getProperty("name"), tmpNode.getDegree());
			}
		}
		System.out.println("LISTSIZE: " + degreeList.size());
		Map<String, Integer> sortedDegreeList = degreeList.entrySet().stream().sorted(Map.Entry.comparingByValue())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
		algoEndTime = System.currentTimeMillis();
		int nodeCounter = sortedDegreeList.size(); 
		if (verbose) {
			for (Map.Entry<String, Integer> entryOfList : sortedDegreeList.entrySet()) {
				System.out.println(entryOfList.getKey() + "\t - " + entryOfList.getValue());
			}
		}
		System.out.println("GOT SORTED DEGREES FOR " + nodeCounter + " NODES IN " + (algoEndTime - algoStartTime) + " ms.");

	}
}
