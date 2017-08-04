package com.bianalysis.server.repo;

import com.bianalysis.server.conf.RedisConfig;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JedisFactory {
    private static Logger LOG = Logger.getLogger(JedisFactory.class.getName());
    private final static int MAX_ACTIVE = 5000;
    private final static int MAX_IDLE = 800;
    private final static int MAX_WAIT = 10000;
    private final static int TIMEOUT = 10 * 1000;

    private static RedisConfig config;

    private static Map<String, JedisPool> jedisPools = new HashMap<String, JedisPool>();

    public static JedisPool initJedisPool() {

        String jedisName = config.jedisName;

        JedisPool jPool = jedisPools.get(jedisName);
        if (jPool == null) {
            String host = config.REDIS_HOST;
            int port = config.REDIS_PORT;
            String[] hosts = host.split(",");
            for (int i = 0; i < hosts.length; i++) {
                try {
                    jPool = newJeisPool(hosts[i], port);
                    if (jPool != null) {
                        break;
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            jedisPools.put(jedisName, jPool);
        }
        return jPool;
    }

    /**
     * 获取一个jedis实例
     *
     * @param filePath
     * @return
     */
    public static Jedis getJedisInstance(String filePath) {
        config = new RedisConfig();
        config.init(filePath);
        LOG.debug("get jedis[name=" + config.jedisName + "]");

        JedisPool jedisPool = jedisPools.get(config.jedisName);
        if (jedisPool == null) {
            jedisPool = initJedisPool();
        }

        Jedis jedis = null;
        for (int i = 0; i < 10; i++) {
            try {
                jedis = jedisPool.getResource();
                break;
            } catch (Exception e) {
                LOG.error("get resource from jedis pool error. times " + (i + 1) + ". retry...", e);
                jedisPool.returnBrokenResource(jedis);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    LOG.warn("sleep error", e1);
                }
            }
        }
        return jedis;
    }

    private static JedisPool newJeisPool(String host, int port) {
        LOG.info("init jedis pool[" + host + ":" + port + "]");
        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnReturn(false);
        config.setTestOnBorrow(false);
        config.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_GROW);
        config.setMaxActive(MAX_ACTIVE);
        config.setMaxIdle(MAX_IDLE);
        config.setMaxWait(MAX_WAIT);
        return new JedisPool(config, host, port, TIMEOUT);
    }

    /**
     * 配合使用getJedisInstance方法后将jedis对象释放回连接池中
     *
     * @param jedis 使用完毕的Jedis对象
     * @return true 释放成功；否则返回false
     */
    public static boolean release(String poolName, Jedis jedis) {
        LOG.debug("release jedis pool[name=" + poolName + "]");

        JedisPool jedisPool = jedisPools.get(poolName);
        if (jedisPool != null && jedis != null) {
            try {
                jedisPool.returnResource(jedis);
            } catch (Exception e) {
                jedisPool.returnBrokenResource(jedis);
            }
            return true;
        }
        return false;
    }

    /**
     * 获取数据
     *
     * @param key
     * @return
     */
    public static String get(String poolName, String key) {

        JedisPool jedisPool = jedisPools.get(poolName);
        String value = null;

        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            value = jedis.get(key);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            returnResource(jedisPool, jedis);
        }

        return value;
    }

    /**
     * 返还到连接池
     *
     * @param pool
     * @param redis
     */
    public static void returnResource(JedisPool pool, Jedis redis) {
        if (redis != null) {
            pool.returnResource(redis);
        }
    }

    public static void destroy() {
        LOG.debug("destroy all pool");
        for (Iterator<JedisPool> itors = jedisPools.values().iterator(); itors.hasNext(); ) {
            try {
                JedisPool jedisPool = itors.next();
                jedisPool.destroy();
            } finally {
            }
        }
    }

    public static void destroy(String poolName) {
        try {
            jedisPools.get(poolName).destroy();
        } catch (Exception e) {
            LOG.warn("destory redis pool[" + poolName + "] error", e);
        }
    }
}
