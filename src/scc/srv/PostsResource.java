package scc.srv;

import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import rx.Observable;
import scc.resources.Like;
import scc.resources.Post;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;

@Path("/post")
public class PostsResource {

    AsyncDocumentClient client;
    CloudBlobContainer container;
    Jedis jedis;
    JedisPool jedisPool;

    public PostsResource() {
        container = new MediaResource().connect();
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

     @POST
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.TEXT_PLAIN)
     public String addPost(Post post) throws Throwable {
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
     post.setDate(LocalDateTime.now().toString());

     String PostsCollection = getCollectionString("Posts");

     Observable<ResourceResponse<Document>> resp = client.createDocument(PostsCollection, post, null, false);

     Document doc = resp.toBlocking().first().getResource();

     try (Jedis jedis = jedisPool.getResource()) {
             Long cnt = jedis.lpush("MostRecentPosts", doc.toJson());
             if (cnt > 10)
             jedis.ltrim("MostRecentPosts", 0, 10);
         }

     return doc.get("id").toString();

     } catch (Exception e) {
         e.printStackTrace();
         throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
     }

     }

    @GET
    @Path("/{postId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String getPost(@PathParam("postId") String postId) {
        try {

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts u WHERE u.id = '" + postId + "'", queryOptions).toBlocking().getIterator();

            String LikesCollection = getCollectionString("Likes");
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it2 = client.queryDocuments(LikesCollection,
                    "SELECT * FROM Posts u WHERE u.postId = '" + postId + "'", queryOptions).toBlocking().getIterator();

            Post post = it.next().getResults().get(0).toObject(Post.class);

            post.setLikes(it2.next().getResults().size());

            Observable<ResourceResponse<Document>> resp =  client.replaceDocument(post.get_self(), post, null);

            Document doc = resp.toBlocking().first().getResource();

            try (Jedis jedis = jedisPool.getResource()) {
                Long cnt = jedis.lpush("MostRecentPosts", doc.toJson());
                if (cnt > 10)
                    jedis.ltrim("MostRecentPosts", 0, 10);
            }

            return doc.toJson();
            //post.setLikes(it2.next().getResults().size());

            //return post.getPostId();
        } catch (Exception e) {
            e.printStackTrace();
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
                    "SELECT * FROM Posts u WHERE u.id = '" + postId + "'", queryOptions).toBlocking().getIterator();

            Document doc = it.next().getResults().get(0);
            RequestOptions options = new RequestOptions();
            options.setPartitionKey( new PartitionKey(doc.get("id").toString()));

            //return it.next().getResults().get(0).getSelfLink();
            Observable<ResourceResponse<Document>> resp = client.deleteDocument(doc.getSelfLink(), options);

            resp.toBlocking().first();
            //resp.subscribe();



        } catch (Exception e) {
            e.printStackTrace();
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }

    }

    @POST
    @Path("/{postId}/like/{userId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void likePost(@PathParam("postId") String postId, @PathParam("userId") String userId) {
        try {

            String LikesCollection = getCollectionString("Likes");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(LikesCollection,
                    "SELECT * FROM Posts u WHERE u.postId = '" + postId + "' AND u.userId = '" + userId + "'", queryOptions).toBlocking().getIterator();

            FeedResponse<Document> doc = it.next();
            Observable<ResourceResponse<Document>> resp;


            if(doc.getResults().size() != 0) {
                RequestOptions options = new RequestOptions();
                options.setPartitionKey( new PartitionKey(postId + userId));

                resp = client.deleteDocument(doc.getResults().get(0).getSelfLink(), options);
            }
            else {
                Like like = new Like();
                like.setPostId(postId);
                like.setUserId(userId);
                like.setCompositeId(postId + userId);
                resp = client.createDocument(LikesCollection, like, null, false );
            }

            resp.subscribe();

        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }

    }

    @GET
    @Path("/test/{postId}/{userId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public int test(@PathParam("postId") String postId, @PathParam("userId") String userId) {
        try {

            String LikesCollection = getCollectionString("Likes");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(LikesCollection,
                    "SELECT * FROM Posts u WHERE u.postId = '" + postId + "' AND u.userId = '" + userId + "'", queryOptions).toBlocking().getIterator();

            return it.next().getResults().size();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
        }
    }

    @GET
    @Path("/test2/{postId}/{userId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public boolean test2(@PathParam("postId") String postId, @PathParam("userId") String userId) {
        try {

            String LikesCollection = getCollectionString("Likes");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(LikesCollection,
                    "SELECT * FROM Posts u WHERE u.postId = '" + postId + "' AND u.userId = '" + userId + "'", queryOptions).toBlocking().getIterator();

            return it.hasNext();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
        }
    }


    @GET
    @Path("/test3/{postId}/{userId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String test3(@PathParam("postId") String postId, @PathParam("userId") String userId) {
        try {

            String LikesCollection = getCollectionString("Likes");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(LikesCollection,
                "COUNT * FROM Posts u WHERE u.postId = '" + postId + "'", queryOptions).toBlocking().getIterator();

            return it.next().getResults().get(0).toJson();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
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

