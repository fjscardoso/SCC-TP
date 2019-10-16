package scc.scc_frontend;

import java.util.Date;
import java.util.Iterator;

import com.google.gson.Gson;
import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

import rx.Observable;
import scc.resources.User;

public class TestAsyncAzure
{
	private static AsyncDocumentClient client;
	
	static synchronized AsyncDocumentClient getDocumentClient() {
		if( client == null) {
			ConnectionPolicy connectionPolicy = new ConnectionPolicy();
			connectionPolicy.setConnectionMode(ConnectionMode.Direct);
			client = new AsyncDocumentClient.Builder()
		         .withServiceEndpoint(TestProperties.COSMOS_DB_ENDPOINT)
		         .withMasterKeyOrResourceToken(TestProperties.COSMOS_DB_MASTER_KEY)
		         .withConnectionPolicy(connectionPolicy)
		         .withConsistencyLevel(ConsistencyLevel.Eventual)
		         .build();
		}
		return client;
	}
	
	/**
	 * Returns the string to access a CosmosDB collection names col
	 * @param col Name of collection
	 * @return
	 */
	static String getCollectionString( String col) {
		return String.format("/dbs/%s/colls/%s", TestProperties.COSMOS_DB_DATABASE, col);		
	}
	

	public static void main( String[] args) {
		try {
		AsyncDocumentClient client = getDocumentClient();
		String UsersCollection = getCollectionString("Users");
				
		User user = new User();
		user.setName("bla" + new Date().getTime());
    	Observable<ResourceResponse<Document>> resp = client.createDocument(UsersCollection, user, null, false);
        String str =  resp.toBlocking().first().getResource().getId();
        System.out.println( str);

		
    	FeedOptions queryOptions = new FeedOptions();
    	queryOptions.setEnableCrossPartitionQuery(true);
    	queryOptions.setMaxDegreeOfParallelism(-1);

    	Iterator<FeedResponse<Document>> it = client.queryDocuments(
    	        UsersCollection, "SELECT u._self FROM Users u",
    	        queryOptions).toBlocking().getIterator();

    	System.out.println( "Result:");
    	while( it.hasNext())
    		for( Document d : it.next().getResults())
    			System.out.println( d.toJson());

    	it = client.queryDocuments(
    	        UsersCollection, "SELECT * FROM Users u WHERE u.id = '2'",
    	        queryOptions).toBlocking().getIterator();

    	System.out.println( "Result:");
    	while( it.hasNext())
    		for( Document d : it.next().getResults()) {
    			System.out.println( d.toJson());
    			Gson g = new Gson();
    			User u = g.fromJson(d.toJson(), User.class);
    			System.out.println( u.getId());
    		}
		} catch( Exception e) {
			e.printStackTrace();
		}
	}

}
