'use strict';

module.exports = {
  genNewUser,
  genNewUserReply,
  genNewCommunity,
  genNewPost,
  genNewPostReply,
  setNewPostImageBody,
  genNewImageReply,
  hasMoreInBrowseList,
  selectAllFromPostList,
  selectFromPostList,
  startBrowse,
  endBrowse
}


const Faker = require('faker')
const fs = require('fs')
const fetch = require('node-fetch')

var userNames = [] 
var userIds = []
var communityNames = []
var postIds = []
var images = []

Array.prototype.sample = function(){
	   return this[Math.floor(Math.random()*this.length)]
}

function random( val){
	return Math.floor(Math.random() * val)
}

function loadData() {
	if( userNames.length > 0)
		return;
	var str;
	if( fs.existsSync('usernames.data')) {
		str = fs.readFileSync('usernames.data','utf8')
		userNames = JSON.parse(str)
	}
	if( fs.existsSync('userids.data')) {
		str = fs.readFileSync('userids.data','utf8')
		userIds = JSON.parse(str)
	}
	if( fs.existsSync('communitynames.data')) {
		str = fs.readFileSync('communitynames.data','utf8')
		communityNames = JSON.parse(str)
	}
	var i
	var basefile
	if( fs.existsSync( '/images')) 
		basefile = '/images/cats.'
	else
		basefile =  'images/cats.'	
	for( i = 1; i <= 40 ; i++) {
		var img  = fs.readFileSync(basefile + i + '.jpeg')
		images.push( img)
	}
}


function genNewUser(context, events, done) {
	const name = `${Faker.name.firstName()}.${Faker.name.lastName()}`
	context.vars.name = name
	userNames.push(name)
	fs.writeFileSync('usernames.data', JSON.stringify(userNames))
	return done()
}


function genNewUserReply(requestParams, response, context, ee, next) {
	if( response.body.length > 0) {
		userIds.push(response.body)
		fs.writeFileSync('userids.data', JSON.stringify(userIds));
	}
    return next()
}

function genNewCommunity(context, events, done) {
	const name = `s/${Faker.lorem.word()}`;
	context.vars.name = name;
	communityNames.push(name);
	fs.writeFileSync('communitynames.data', JSON.stringify(communityNames));
	return done()
}


function genNewPost(context, events, done) {
	loadData();
	context.vars.community = communityNames.sample()
	context.vars.creator = userNames.sample()
	context.vars.msg = `${Faker.lorem.paragraph()}`;
	if( postIds.length > 0 && Math.random() < 0.8) {  // 80% are replies
		context.vars.parentId = postIds.sample()
	} else {
		context.vars.parentId = null
	}
	context.vars.hasImage = false 
	if(Math.random() < 0.2) {   // 20% of the posts have images
		context.vars.image = images.sample()
		context.vars.hasImage = true 
	}
	return done()
}

function setNewPostImageBody(requestParams, context, ee, next) {
	if( context.vars.hasImage)  {
		requestParams.body = context.vars.image
	}
	return next()
}

function genNewImageReply(requestParams, response, context, ee, next) {
	if( response.body && response.body.length > 0) {
		context.vars.imageId = response.body
	}
    return next()
}


function genNewPostReply(requestParams, response, context, ee, next) {
	if( response.body && response.body.length > 0) {
		postIds.push(response.body)
	}
    return next()
}

function startBrowse(context, events, done) {
	context.vars.idstoread = []
	context.vars.browsecount = 0
	return done()
}

function hasMoreInBrowseList(context, next) {
	if( context.vars.idstoread.length > 0) {
		context.vars.nextid = context.vars.idstoread.splice(-1,1)[0]
	    context.vars.hasNextid = true
	    context.vars.browsecount++
	} else {
		context.vars.hasNextid = false
	}
	return next(context.vars.hasNextid)
}

function endBrowse(context, next) {
	const continueLooping = random(100) > context.vars.browsecount
	return next(context.vars.idstoread.length > 0 && continueLooping)
}

function selectFromPostList(requestParams, response, context, ee, next) {
	if( response.body && response.body.length > 0) {
		var resp = JSON.parse( response.body)
		var num = random(resp.posts.length)
		var i
		for( i = 0 ; i < num; i ++) {
			context.vars.idstoread.push(resp.posts[random(resp.posts.length)].id)
		}
	}
	if( context.vars.idstoread.length > 0) {
		context.vars.curid = context.vars.nextid
		context.vars.nextid = context.vars.idstoread.splice(-1,1)[0]
	    context.vars.hasNextid = true
	    context.vars.browsecount++
	} else {
		context.vars.hasNextid = false
	}
	if( random(100) < 33) {
	    context.vars.like = true
	    context.vars.likeUser = userNames.sample()
	} else 
	    context.vars.like = false
	if( random(100) < 25) 
	    context.vars.reply = true
	else 
	    context.vars.reply = false
    return next()
}

function selectAllFromPostList(requestParams, response, context, ee, next) {
	if( response.body && response.body.length > 0) {
		var resp = JSON.parse( response.body)
		var i
		for( i = 0 ; i < resp.posts.length; i ++) {
			context.vars.idstoread.push(resp.posts[i].id)
		}
	}
	if( context.vars.idstoread.length > 0) {
		context.vars.nextid = context.vars.idstoread.splice(-1,1)[0]
	    context.vars.hasNextid = true
	    context.vars.browsecount++
	} else {
		context.vars.hasNextid = false
	}
    return next()
}

