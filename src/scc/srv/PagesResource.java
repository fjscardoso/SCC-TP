package scc.srv;

import com.microsoft.azure.cosmosdb.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;

import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import scc.resources.Post;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Duration;
import java.util.*;

@Path("/pages")
public class PagesResource {

    AsyncDocumentClient client;

    public PagesResource() {

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
    @Path("/initial")
    @Produces(MediaType.TEXT_PLAIN)
    public String getInitial() {
        try {

           try (Jedis jedis = RedisCache.getCache().getJedisPool().getResource()) {
                String posts = jedis.get("serverlesscosmos");
                return posts;
            }

/**            List<Post> posts = new LinkedList<>();

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts OFFSET 0 LIMIT 10", queryOptions).toBlocking().getIterator();

            FeedResponse<Document> feed = it.next();

            for(int i = 0; i < feed.getResults().size(); i++) {
                Post doc = feed.getResults().get(i).toObject(Post.class);
                posts.add(doc);
            }


            return posts;
 */

        } catch (Exception e) {
            e.printStackTrace();
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }
    }

    @GET
    @Path("/thread/{id}")
    @Produces(MediaType.TEXT_PLAIN)
    public List<String> getThread(@PathParam("id") String id) {
        try {

            try (Jedis jedis = RedisCache.getCache().getJedisPool().getResource()) {
                //return jedis.lrange(id, 0, 9);
                return new LinkedList<>(jedis.hgetAll(id).values());
            }

/**            List<String> list = new LinkedList<>();

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts u WHERE u.id = '" + id + "' OR u.parentId = '" + id + "'", queryOptions).toBlocking().getIterator();

            FeedResponse<Document> feed = it.next();

            try (Jedis jedis = jedisPool.getResource()) {

                for (int i = 0; i < feed.getResults().size(); i++) {
                    String doc = feed.getResults().get(i).toJson();
                    list.add(doc);
                    jedis.lpush(id, doc);
                }
            }

            return list;
 */

        } catch (Exception e) {
            e.printStackTrace();
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

