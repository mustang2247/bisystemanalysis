package com.bianalysis.server;

import com.bianalysis.server.db.repo.RepoManager;
import com.bianalysis.server.db.sql.SqlCommandRegistry;
import com.bianalysis.server.storm.BiTopology;
import com.bianalysis.server.utils.PropUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Starter {
    private static final Logger logger = LoggerFactory.getLogger(Starter.class);

    public static void main(String[] args) throws Exception {

        try {
            String envDir = PropUtil.getProp("/env.properties", "envdir");

            // sql 命令初始化
            SqlCommandRegistry.getInstance().init("/" + envDir + "sql.xml");
            // init mysql datasource
            RepoManager.init("/" + envDir + "mysql.properties");
            // redis init
//            JedisFactory.getJedisInstance("/" + envDir + "redis.properties");

            BiTopology.init(args);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException("Starter init fail");
        }

        logger.info("main init OK");
    }

}
