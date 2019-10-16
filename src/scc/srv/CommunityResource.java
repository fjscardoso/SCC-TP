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
import java.util.LinkedList;
import java.util.List;

import static scc.srv.PostsResource.getCollectionString;

@Path("/r")
public class CommunityResource {

    AsyncDocumentClient client;
    CloudBlobContainer container;


    @GET
    @Path("/{communityId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public List<String> getPosts(@PathParam("communityId") String communityId) {
        try {

            List<String> list = new LinkedList<>();

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts u WHERE u.id = '" + communityId + "'", queryOptions).toBlocking().getIterator();

            while(it.hasNext()) {
                String doc = it.next().getResults().get(0).toJson();
                list.add(doc);
            }
            return list;

        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());
        }
    }
}

