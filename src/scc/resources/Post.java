package scc.resources;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Post
{

    //community (string), creator (string),
    //time of creation (date), text of message (string),
    //link to one multimedia object (string, optional),
    //reference to the parent post (string, optional)

    private String community;
    private String postId;
    private String creator;
    private String content;
    private String uId;
    private byte[] image;
    private int likes;

    public String getId() {
        return postId;
    }

    public void setId(String id) {
        this.postId = postId;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getCommunity() {
        return community;
    }

    public void setCommunity(String community) {
        this.community = community;
    }

    public String getuId() {
        return uId;
    }

    public void setuId(String uId) {
        this.uId = uId;
    }

    public byte[] getImage() {
        return image;
    }

    public void setImage(byte[] image) {
        this.image = image;
    }

    public int getLikes() {
        return likes;
    }

    public void setLikes(int likes) {
        this.likes = likes;
    }
}

