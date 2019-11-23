package scc.srv;

import scc.resources.Post;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

@ApplicationPath("/reddit/")
public class MainApplication extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> set = new HashSet<>();
        set.add( MediaResource.class );
        set.add( UsersResource.class );
        set.add( PostsResource.class );
        set.add( CommunityResource.class );
        set.add( PagesResource.class );
        return set;
    }
}