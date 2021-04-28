package dbPackage;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.configuration.*;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.factory.*;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;


import org.neo4j.kernel.*;

public class dbController<managementService> {
	
    private GraphDatabaseService graphDB;
    private DatabaseManagementService managementService;
    Relationship relationship;
	
//    Constrctor
	/**
	 * 
	 */
	public dbController(File dbDirectory, GraphDatabaseService graphDB){
		try {
			System.out.println("FIRST: " + graphDB);
			graphDB = createDb(dbDirectory);
			System.out.println("DATABASE CREATED");
			System.out.println("SECOND: " + graphDB);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * This method returns a new database instance which is created within the 
	 * @param dbDirectory
	 * @return
	 * @throws IOException
	 */
	private GraphDatabaseService createDb(File dbDirectory) throws IOException{
        managementService = new DatabaseManagementServiceBuilder( dbDirectory ).build();
        GraphDatabaseService newgraphDB = managementService.database( DEFAULT_DATABASE_NAME );
        registerShutdownHook( managementService );
        return newgraphDB;
    }
	
		
    /**
     * Registers a shutdown hook for the Neo4j instance so that it
     * shuts down nicely when the VM exits (even if you "Ctrl-C" the
     * running application).
     * @param managementService
     */
    private static void registerShutdownHook( final DatabaseManagementService managementService )
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                managementService.shutdown();
            }
        } );
    }


	/**
	 * This method shuts down all database instances. 
	 */
	public void shutDown() {
		registerShutdownHook(managementService);
		System.out.println("DATABASE SHUTDOWN");
	}
}
