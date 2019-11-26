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
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

@Path("/post")
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
     public String addPost(Post post) throws Throwable {

     try {

         if(client == null)
             connect();

         String CommunitiesCollection = getCollectionString("Communities");
         FeedOptions queryOptions = new FeedOptions();
         queryOptions.setEnableCrossPartitionQuery(true);
         queryOptions.setMaxDegreeOfParallelism(-1);
         Iterator<FeedResponse<Document>> existsCommunity = client.queryDocuments(CommunitiesCollection,
                 "SELECT * FROM Communities u WHERE u.name = '" + post.getCommunity() + "'", queryOptions).toBlocking().getIterator();

         if(existsCommunity.next().getResults().size() == 0)
             throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());

         String UsersCollection = getCollectionString("Users");
         Iterator<FeedResponse<Document>> existsUser = client.queryDocuments(UsersCollection,
                 "SELECT * FROM Users u WHERE u.name = '" + post.getCreator() + "'", queryOptions).toBlocking().getIterator();

         if(existsUser.next().getResults().size() == 0)
             throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());

         post.setDate(LocalDateTime.now().toString());

        String PostsCollection = getCollectionString("Posts");

        Observable<ResourceResponse<Document>> resp = client.createDocument(PostsCollection, post, null, false);

        Document doc = resp.toBlocking().first().getResource();

/**         try (Jedis jedis = RedisCache.getCache().getJedisPool().getResource()) {
             Long cnt = jedis.lpush(doc.get("id").toString(), doc.toJson());
             if (cnt > 10)
                 jedis.ltrim(doc.get("id").toString(), 0, 9);

             if(!post.getParentId().isEmpty())
                 cnt = jedis.lpush(post.getParentId(), doc.toJson());
             if (cnt > 10)
                 jedis.ltrim(post.getParentId(), 0, 9);

         }
*/
         try (Jedis jedis = RedisCache.getCache().getJedisPool().getResource()) {
             jedis.hset(doc.get("id").toString(), doc.get("id").toString(), doc.toJson());

             if(!post.getParentId().isEmpty())
                 jedis.hset(post.getParentId(), doc.get("id").toString(), doc.toJson());

         }

     return doc.get("id").toString();

     } catch (Exception e) {
         e.printStackTrace();
         throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
     }

     }

    @GET
    @Path("/{postId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Post getPost(@PathParam("postId") String postId) {
        try {

            if(client == null)
                connect();

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts u WHERE u.id = '" + postId + "'", queryOptions).toBlocking().getIterator();


            Post post = it.next().getResults().get(0).toObject(Post.class);

            return post;

/**            try (Jedis jedis = jedisPool.getResource()) {
                Long cnt = jedis.lpush("MostRecentPosts", doc.toJson());
                if (cnt > 10)
                    jedis.ltrim("MostRecentPosts", 0, 10);

            }
*/

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

            if(client == null)
                connect();

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts u WHERE u.id = '" + postId + "'", queryOptions).toBlocking().getIterator();

            Document doc = it.next().getResults().get(0);
            RequestOptions options = new RequestOptions();
            options.setPartitionKey( new PartitionKey(doc.get("id").toString()));

            Observable<ResourceResponse<Document>> resp = client.deleteDocument(doc.getSelfLink(), options);

            resp.toBlocking().first();
            //TODO
            try (Jedis jedis = RedisCache.getCache().getJedisPool().getResource()) {
                List<String> list = new LinkedList<>();
                list.addAll(jedis.hgetAll(postId).keySet());
                for(int i = 0;i < list.size(); i++)
                    jedis.hdel(postId, list.get(i));

                //jedis.hdel(doc.get("parentId").toString(), postId);

            }


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

            if(client == null)
                connect();

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> existsPost = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts u WHERE u.id = '" + postId + "'", queryOptions).toBlocking().getIterator();

            if(existsPost.next().getResults().size() == 0)
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());

            String UsersCollection = getCollectionString("Users");
            Iterator<FeedResponse<Document>> existsUser = client.queryDocuments(UsersCollection,
                    "SELECT * FROM Users u WHERE u.name = '" + userId + "'", queryOptions).toBlocking().getIterator();

            if(existsUser.next().getResults().size() == 0)
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());

            String LikesCollection = getCollectionString("Likes");
           //FeedOptions queryOptions = new FeedOptions();
           //queryOptions.setEnableCrossPartitionQuery(true);
            // queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(LikesCollection,
                    "SELECT * FROM Likes u WHERE u.postId = '" + postId + "' AND u.userId = '" + userId + "'", queryOptions).toBlocking().getIterator();

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

            replacePost(postId);

        } catch (Exception e) {
            e.printStackTrace();
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }

    }

    private void replacePost(String postId){

            String PostsCollection = getCollectionString("Posts");
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true);
            queryOptions.setMaxDegreeOfParallelism(-1);
            Iterator<FeedResponse<Document>> it = client.queryDocuments(PostsCollection,
                    "SELECT * FROM Posts u WHERE u.id = '" + postId + "'", queryOptions).toBlocking().getIterator();

            String LikesCollection = getCollectionString("Likes");
            Iterator<FeedResponse<Document>> it2 = client.queryDocuments(LikesCollection,
                    "SELECT * FROM Likes u WHERE u.postId = '" + postId + "'", queryOptions).toBlocking().getIterator();

            Post post = it.next().getResults().get(0).toObject(Post.class);

            post.setLikes(it2.next().getResults().size());

            Observable<ResourceResponse<Document>> resp = client.replaceDocument(post.get_self(), post, null);

            Document doc = resp.toBlocking().first().getResource();

        try (Jedis jedis = RedisCache.getCache().getJedisPool().getResource()) {
            jedis.hdel(postId, postId);
            jedis.hset(postId, postId, doc.toJson());
            if(!post.getParentId().isEmpty()) {
                jedis.hdel(post.getParentId(), post.getId());
                jedis.hset(post.getParentId(), post.getId(), doc.toJson());
            }

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

