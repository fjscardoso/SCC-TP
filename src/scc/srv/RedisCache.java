package scc.srv;


import java.time.Duration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scc.utils.AzureProperties;

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
		String RedisHostname = AzureProperties.getProperties().getProperty(AzureProperties.REDIS_URL);
		String cacheKey = AzureProperties.getProperties().getProperty(AzureProperties.REDIS_KEY);
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
		jedisPool = new JedisPool(poolConfig, RedisHostname, 6380, 1000, cacheKey, true);
	}
	 
	public JedisPool getJedisPool() {
		return jedisPool;
	}
	
	
	

	
}


