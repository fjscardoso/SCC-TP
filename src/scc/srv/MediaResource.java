package scc.srv;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;

@Path("/media")
public class MediaResource {

    String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=scc42997;AccountKey=IUDnP8H4FgPOrVu3BoQN9VEUHPUw8osO8GS5wLVCD3OklK32H3v5Kvf0P3guD9k11A9b7T4xC/LjDaFdtWPOWw==;EndpointSuffix=core.windows.net";
    CloudBlobContainer container;
    CloudBlobClient blobClient;

    public void connect() throws URISyntaxException, StorageException, InvalidKeyException {
        try {
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
            blobClient = storageAccount.createCloudBlobClient();
            container = blobClient.getContainerReference("images");
        }
        catch(Exception e){
            container = null;
        }

    }

    @Path("/{name}")
    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public String createContainer(@PathParam("name") String name) throws StorageException, URISyntaxException, InvalidKeyException {

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


    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public String upload( byte[] contents) throws StorageException, URISyntaxException, InvalidKeyException {
            if(container == null)
                connect();

            try {
                String hash = "" + contents.hashCode();

                CloudBlob blob = container.getBlockBlobReference(hash);

                blob.uploadFromByteArray(contents, 0, contents.length);

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
    public byte[] download(@PathParam("uid") String uid) throws URISyntaxException, StorageException, InvalidKeyException {

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
    public void delete(@PathParam("uid") String uid) throws URISyntaxException, StorageException, InvalidKeyException {

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
    public List<String> list() throws StorageException, InvalidKeyException, URISyntaxException {

        if(container == null)
            connect();

        List<String> aux = new ArrayList<>();

        for (ListBlobItem blobItem : container.listBlobs())
            aux.add(blobItem.getUri().toString());

        return aux;

    }
}
