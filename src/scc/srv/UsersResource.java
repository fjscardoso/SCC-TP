package scc.srv;

import com.microsoft.azure.cosmosdb.*;
import rx.Observable;
import scc.resources.User;

import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

@Path("/user")
public class UsersResource {

    AsyncDocumentClient client;

    public UsersResource() {
        connect();
    }


    public void connect() {
        ConnectionPolicy connectionPolicy = ConnectionPolicy.GetDefault();
        connectionPolicy.setConnectionMode(ConnectionMode.Direct);
        client = new AsyncDocumentClient.Builder()
                .withServiceEndpoint(TestProperties.COSMOS_DB_ENDPOINT)
                .withMasterKeyOrResourceToken(TestProperties.COSMOS_DB_MASTER_KEY)
                .withConnectionPolicy(connectionPolicy)
                .withConsistencyLevel(ConsistencyLevel.Eventual).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String addUser(User input) {
        try {
            User user = input;
            String UsersCollection = getCollectionString("Users");
            Observable<ResourceResponse<Document>> resp = client.createDocument(UsersCollection, user, null, false);
            return resp.toBlocking().first().getResource().getSelfLink();

        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());
        }

    }

    @GET
    @Path("/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String getUser(@PathParam("name") String name) {
        try {

            String UsersCollection = getCollectionString("Users");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(UsersCollection,
                    "SELECT * FROM Users u WHERE u.name = '" + name + "'", queryOptions).toBlocking().getIterator();

            String doc = it.next().getResults().get(0).toJson();

            return doc;

        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }
    }

    @DELETE
    @Path("/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void deleteUser(@PathParam("name") String name) {

        String UsersCollection = getCollectionString("Users");
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.setEnableCrossPartitionQuery(true);
        queryOptions.setMaxDegreeOfParallelism(-1);
        Iterator<FeedResponse<Document>> it = client.queryDocuments(UsersCollection,
                "SELECT * FROM Users u WHERE u.name = '" + name + "'", queryOptions).toBlocking().getIterator();

        Document doc = it.next().getResults().get(0);
        RequestOptions options = new RequestOptions();
        options.setPartitionKey( new PartitionKey(doc.get("name").toString()));

        client.deleteDocument(doc.getSelfLink(), options);


    }


    /**
     * Returns the string to access a CosmosDB collection names col
     *
     * @param col Name of collection
     * @return
     */
    static String getCollectionString(String col) {
        return String.format("/dbs/%s/colls/%s", TestProperties.COSMOS_DB_DATABASE, col);
    }

}

