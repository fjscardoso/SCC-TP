package scc.srv;

import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import rx.Observable;
import scc.resources.Community;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static scc.srv.PostsResource.getCollectionString;

@Path("/community")
public class CommunityResource {

    AsyncDocumentClient client;

    public CommunityResource() {
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


    @GET
    @Path("/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getPosts(@PathParam("name") String name) {
        try {

            if(client == null)
                connect();

            List<String> list = new LinkedList<>();

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts u WHERE u.community = '" + name + "'", queryOptions).toBlocking().getIterator();

            FeedResponse<Document> feed = it.next();

            for(int i = 0; i < feed.getResults().size(); i++) {
                String doc = feed.getResults().get(i).toJson();
                list.add(doc);
            }
            return list;

        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String createCommunity(Community community) {
        try {

            if(client == null)
                connect();

            String CommunitiesCollection = getCollectionString("Communities");
            Observable<ResourceResponse<Document>> resp = client.createDocument(CommunitiesCollection, community, null, false);
            return resp.toBlocking().first().getResource().getSelfLink();
        } catch (Exception e) {
            e.printStackTrace();
            throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());
        }

    }

    @DELETE
    @Path("/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void deleteCommunity(@PathParam("name") String name) {
        try {

            if(client == null)
                connect();

            String CommunityCollection = getCollectionString("Communities");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(CommunityCollection,
                    "SELECT * FROM Posts u WHERE u.name = '" + name + "'", queryOptions).toBlocking().getIterator();

            Document doc = it.next().getResults().get(0);
            RequestOptions options = new RequestOptions();
            options.setPartitionKey( new PartitionKey(doc.get("name").toString()));

            Observable<ResourceResponse<Document>> resp = client.deleteDocument(doc.getSelfLink(), options);

            resp.toBlocking().first();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.CONFLICT).build());
        }

    }

}

