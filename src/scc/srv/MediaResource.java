package scc.srv;

import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import rx.Observable;
import scc.resources.Post;
import scc.scc_frontend.TestProperties;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Path("/media")
public class MediaResource {

    String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=scc42997;AccountKey=yPh/Eit89QCMxoxfs0O05fG5T/hHnwy/qbaXFwieWupDMl9K6Qn6VxdumNDiFwXW/rIZqlzN/UDwzvBembnCVw==;EndpointSuffix=core.windows.net";
    CloudBlobContainer container;
    CloudBlobClient blobClient;
    AsyncDocumentClient client;

    public MediaResource(){

        connect();

        ConnectionPolicy connectionPolicy = ConnectionPolicy.GetDefault();
        connectionPolicy.setConnectionMode(ConnectionMode.Direct);
        client = new AsyncDocumentClient.Builder()
                .withServiceEndpoint(TestProperties.COSMOS_DB_ENDPOINT)
                .withMasterKeyOrResourceToken(TestProperties.COSMOS_DB_MASTER_KEY)
                .withConnectionPolicy(connectionPolicy)
                .withConsistencyLevel(ConsistencyLevel.Eventual).build();
    }

    public CloudBlobContainer connect() {
        try {
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
            blobClient = storageAccount.createCloudBlobClient();
            container = blobClient.getContainerReference("images");
            return container;
        }
        catch(Exception e){
            container = null;
            return container;
        }

    }

    @Path("/{name}")
    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public String createContainer(@PathParam("name") String name) {

        if(container == null)
            connect();

        try {

            container = blobClient.getContainerReference(name);
            if(!container.createIfNotExists())
                throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());

        }
        catch(Exception e){
        };

        return null;

    }

    @Path("/upload/{postId}")
    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public String upload(@PathParam("postId") String postId, byte[] contents) {
            if(container == null)
                connect();

            try {
                String hash = "" + contents.hashCode();

                CloudBlob blob = container.getBlockBlobReference(hash);

                blob.uploadFromByteArray(contents, 0, contents.length);

                String PostsCollection = getCollectionString("Posts");
                FeedOptions queryOptions = new FeedOptions();
                queryOptions.setEnableCrossPartitionQuery(true);
                queryOptions.setMaxDegreeOfParallelism(-1);
                Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                        "SELECT * FROM Posts u WHERE u.postId = '" + postId + "'", queryOptions).toBlocking().getIterator();

                Post post = it.next().getResults().get(0).toObject(Post.class);

                post.setFileId(hash);

                Observable<ResourceResponse<Document>> resp = client.replaceDocument(post.get_self(), post, null);

                resp.subscribe();

                return hash;



            }
            catch(Exception x){
                throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());

            }

    }

    @Path("/{uid}")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public byte[] download(@PathParam("uid") String uid) {

        if(container == null)
            connect();

        try {
            CloudBlob blob = container.getBlobReferenceFromServer(uid);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            blob.download(out);
            out.close();
            byte[] contents = out.toByteArray();

            return contents;
        }
        catch(Exception e){}

        return null;
    }

    @Path("/{uid}")
    @DELETE
    public void delete(@PathParam("uid") String uid) {

        if(container == null)
            connect();

        try {
            CloudBlob blob = container.getBlobReferenceFromServer(uid);
            if(!blob.deleteIfExists())
                throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());
        }
        catch(Exception e){}

    }

    @Path("/list")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> list() {

        if(container == null)
            connect();

        List<String> aux = new ArrayList<>();

        for (ListBlobItem blobItem : container.listBlobs())
            aux.add(blobItem.getUri().toString());

        return aux;

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
