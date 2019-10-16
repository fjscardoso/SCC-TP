package scc.srv;

import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import rx.Observable;
import scc.resources.Post;
import scc.scc_frontend.TestProperties;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Iterator;

@Path("/posts")
public class PostsResource {

    AsyncDocumentClient client;
    CloudBlobContainer container;

    public PostsResource() {
        container = new MediaResource().connect();
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
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.TEXT_PLAIN)
    public String addPost(Post post) {
        try {

            String PostsCollection = getCollectionString("Posts");

            String hash = "" + post.getImage().hashCode();
            post.setuId(hash);

            Observable<ResourceResponse<Document>> resp = client.createDocument(PostsCollection, post, null, false);

            CloudBlob blob = container.getBlockBlobReference(hash);

            blob.uploadFromByteArray(post.getImage(), 0, post.getImage().length);

            return resp.toBlocking().first().getResource().getSelfLink();

        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());
        }

    }

    @GET
    @Path("/{postId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String getPost(@PathParam("postId") String name) {
        try {

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts u WHERE u.id = '" + name + "'", queryOptions).toBlocking().getIterator();

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
    public void deletePost(@PathParam("name") String name) {

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

