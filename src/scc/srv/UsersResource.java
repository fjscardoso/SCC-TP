package scc.srv;

import com.microsoft.azure.cosmosdb.*;
import rx.Observable;
import scc.resources.User;
import scc.scc_frontend.TestProperties;

import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;

import javax.ws.rs.*;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;

@Path("/users")
public class UsersResource {

    AsyncDocumentClient client;
    Map<String, List<String>> userPosts = new HashMap<>();

    public UsersResource() throws StorageException, InvalidKeyException, URISyntaxException {
        connect();
    }


    public void connect() throws URISyntaxException, StorageException, InvalidKeyException {
        ConnectionPolicy connectionPolicy = ConnectionPolicy.GetDefault();
        connectionPolicy.setConnectionMode(ConnectionMode.Direct);
        client = new AsyncDocumentClient.Builder()
                .withServiceEndpoint(TestProperties.COSMOS_DB_ENDPOINT)
                .withMasterKeyOrResourceToken(TestProperties.COSMOS_DB_MASTER_KEY)
                .withConnectionPolicy(connectionPolicy)
                .withConsistencyLevel(ConsistencyLevel.Eventual).build();
    }

    @POST
    @Path("/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String addUser(@PathParam("name") String name) {
        try {
            User user = new User();
            user.setName(name);
            String UsersCollection = getCollectionString("Users");
            Observable<ResourceResponse<Document>> resp = client.createDocument(UsersCollection, user, null, false);
            String a = resp.toBlocking().first().getResource().getSelfLink();
            userPosts.get(name).add(a);
            return a;
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
            FeedOptions queryOptions = new FeedOptions(); queryOptions.setEnableCrossPartitionQuery(true); queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments( UsersCollection,
                    "SELECT * FROM Users u WHERE u.name = '" + name + "'", queryOptions).toBlocking().getIterator();

            String doc = it.next().getResults().get(0).toJson();

                return doc;
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());
        }
    }

    @DELETE
    @Path("/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void deleteUser(@PathParam("name") String name) {
        try {

            for(String x : userPosts.get(name))
                client.deleteDocument(x, null);

        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());
        }
    }


    /**
     * Returns the string to access a CosmosDB collection names col
     * @param col Name of collection
     * @return
     */
    static String getCollectionString( String col) {
        return String.format("/dbs/%s/colls/%s", TestProperties.COSMOS_DB_DATABASE, col);
    }

}

