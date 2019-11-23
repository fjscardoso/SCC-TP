package scc.srv;

import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import rx.Observable;
import scc.resources.Post;
import scc.resources.User;

import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

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
        createCache();
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
    @Produces(MediaType.TEXT_PLAIN)
    public List<String> getInitial() {
        try {

            List<String> posts;

            try (Jedis jedis = jedisPool.getResource()) {
                posts = jedis.lrange("MostRecentPosts", 0, 9);
            }

            return posts;

        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }
    }

    @GET
    @Path("/thread/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getThread(@PathParam("id") String id) {
        try {

            List<String> list = new LinkedList<>();

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Users u WHERE u.id = '" + id + "'", queryOptions).toBlocking().getIterator();

            list.add(it.next().getResults().get(0).toJson());

            Iterator<FeedResponse<Document>> it2 = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Users u WHERE u.parentId = '" + id + "'", queryOptions).toBlocking().getIterator();

            FeedResponse<Document> feed = it2.next();

            for(int i = 0; i < feed.getResults().size(); i++) {
                String doc = feed.getResults().get(i).toJson();
                list.add(doc);
            }


            return list;

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

