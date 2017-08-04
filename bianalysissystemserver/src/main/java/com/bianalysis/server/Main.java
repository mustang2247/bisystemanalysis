package com.bianalysis.server;

import com.bianalysis.server.repo.RepoManager;
import com.bianalysis.server.sql.SqlCommandRegistry;
import com.bianalysis.server.utils.PropUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        try {
            String envDir = PropUtil.getProp("/env.properties", "envdir");
            logger.info("envdir: " + envDir);

            // sql 命令初始化
            SqlCommandRegistry.getInstance().init("/" + envDir + "sql.xml");
            RepoManager.init("/" + envDir + "mysql.properties");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException("BiServerInitializer init fail");
        }

        logger.info("main init OK");
    }

}
