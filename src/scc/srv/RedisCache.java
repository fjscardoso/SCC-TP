package scc.srv;


import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

public class RedisCache
{
	static RedisCache cache;
	public static RedisCache getCache() {
		if( cache == null) {
			cache = new RedisCache();
		}
		return cache;
	}

	private JedisPool jedisPool;
	
	RedisCache() {
	    final JedisPoolConfig poolConfig = new JedisPoolConfig();
	    poolConfig.setMaxTotal(128);
	    poolConfig.setMaxIdle(128);
	    poolConfig.setMinIdle(16);
	    poolConfig.setTestOnBorrow(true);
	    poolConfig.setTestOnReturn(true);
	    poolConfig.setTestWhileIdle(true);
	    poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
	    poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
	    poolConfig.setNumTestsPerEvictionRun(3);
	    poolConfig.setBlockWhenExhausted(true);
		jedisPool = new JedisPool(poolConfig, TestProperties.REDIS_HOSTNAME, 6380, 1000, TestProperties.REDIS_KEY, true);
	}
	 
	public JedisPool getJedisPool() {
		return jedisPool;
	}
	
	
	

	
}


