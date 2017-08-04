package com.bianalysis.server.repo;

import com.bianalysis.server.utils.PropUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * @Date Aug 6, 2015
 * @Author dengjie
 * @Note TODO
 */
public class TestJedis {

    private static final Logger logger = LoggerFactory.getLogger(TestMysql.class);

    @Test
    public void testRedis() {
        String envDir = PropUtil.getProp("/env.properties", "envdir");

        Jedis jedis = JedisFactory.getJedisInstance("/" + envDir + "redis.properties");
        jedis.set("20170804" + "_" + "jedis", "1");
        jedis.flushAll();
    }

}
