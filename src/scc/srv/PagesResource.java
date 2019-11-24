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
    Jedis jedis;
    JedisPool jedisPool;

    public PagesResource() {

        connect();
        createCache();
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

    public void createCache(){
        JedisShardInfo shardInfo = new JedisShardInfo(TestProperties.REDIS_HOSTNAME, 6380, true);
        shardInfo.setPassword(TestProperties.REDIS_KEY);
        jedis = new Jedis(shardInfo);

        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        poolConfig.setNumTestsPerEvictionRun(3); poolConfig.setBlockWhenExhausted(true);
        jedisPool = new JedisPool(poolConfig, TestProperties.REDIS_HOSTNAME, 6380, 1000, TestProperties.REDIS_KEY, true);
    }

    @GET
    @Path("/initial")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Post> getInitial() {
        try {

/**            try (Jedis jedis = jedisPool.getResource()) {
                posts = jedis.lrange("serverlesscosmos"", 0, 9);
            }
*/

            List<Post> posts = new LinkedList<>();

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

            try (Jedis jedis = jedisPool.getResource()) {
                if(jedis.llen(id) != 0) {
                    return jedis.lrange(id, 0, 9);
                }
            }

            List<String> list = new LinkedList<>();

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

