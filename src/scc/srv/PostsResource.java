package scc.srv;

import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.glassfish.jersey.media.multipart.FormDataParam;
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
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.TEXT_PLAIN)
     public String addPost(Post post) {
     try {

/**     String UsersCollection = getCollectionString("Users");
     FeedOptions queryOptions = new FeedOptions();
     queryOptions.setEnableCrossPartitionQuery(true);
     queryOptions.setMaxDegreeOfParallelism(-1);
     Iterator<FeedResponse<Document>> it = client.queryDocuments(UsersCollection,
     "SELECT * FROM Users u WHERE u.name = '" + post.getUserId() + "'", queryOptions).toBlocking().getIterator();

     if(it.next().getResults().get(0).toJson().equals(null))
     throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
*/
     String postId = "" + (post.getUserId() + post.getCommunity() +  post.getTitle()).hashCode();
     post.setPostId(postId);

     String PostsCollection = getCollectionString("Posts");

     Observable<ResourceResponse<Document>> resp = client.createDocument(PostsCollection, post, null, false);

     return resp.toBlocking().first().getResource().getSelfLink();
         // return postId;

     } catch (Exception e) {
     throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());
     }

     }

    @GET
    @Path("/{postId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String getPost(@PathParam("postId") String postId) {
        try {

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts u WHERE u.postId = '" + postId + "'", queryOptions).toBlocking().getIterator();


            Post post = it.next().getResults().get(0).toObject(Post.class);

            return post.getPostId();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }
    }

    @DELETE
    @Path("/{postId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void deletePost(@PathParam("postId") String postId) {
        try {

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts u WHERE u.postId = '" + postId + "'", queryOptions).toBlocking().getIterator();

            client.deleteDocument(it.next().getResults().get(0).getSelfLink(), null);

        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }

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

