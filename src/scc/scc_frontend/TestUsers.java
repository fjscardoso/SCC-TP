package scc.scc_frontend;

import java.net.URI;
import java.util.Date;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.client.*;

import com.google.gson.Gson;

import scc.resources.User;

/**
 * Hello world!
 *
 */
public class TestUsers
{
	public static void main(String[] args) {

		try {
			String hostname = "https://scc-backend-4204.azurewebsites.net/";
			ClientConfig config = new ClientConfig();
			Client client = ClientBuilder.newClient(config);

			URI baseURI = UriBuilder.fromUri(hostname).build();

			WebTarget target = client.target(baseURI);
			
			User user = new User();
			user.setName( "user-" + new Date().getTime());
			
			String id = target.path("/user").request().accept(MediaType.TEXT_PLAIN)
					.post(Entity.entity(user,MediaType.APPLICATION_JSON)).readEntity(String.class);

			User u0 = target.path("/user/" + id).request().accept(MediaType.APPLICATION_JSON)
					.get( User.class);
			System.out.println( u0.getId());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


