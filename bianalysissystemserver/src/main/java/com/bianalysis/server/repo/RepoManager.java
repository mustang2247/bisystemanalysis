package com.bianalysis.server.repo;

import com.bianalysis.server.conf.MysqlCfg;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.sql.SQLException;

public class RepoManager {
    private static final Logger logger = LoggerFactory.getLogger(RepoManager.class);

    private static ComboPooledDataSource cpds;
    private static BiRepo biRepo;
    private static SqlRepo sqlRepo;

    public static boolean init( String filePath ) throws PropertyVetoException, SQLException {
        MysqlCfg config = new MysqlCfg();
        config.init( filePath );

        cpds = new ComboPooledDataSource("bianalysissystemserver");
        cpds.setDriverClass( "com.mysql.jdbc.Driver" );
        cpds.setJdbcUrl( "jdbc:mysql://" + config.ADDR + "/" + config.DB_NAME + "?useUnicode=true&characterEncoding=utf-8" );
        cpds.setUser( config.LOGIN_NAME );
        cpds.setPassword( config.LOGIN_PASS );
        cpds.setInitialPoolSize( config.MAX_CONN );
        cpds.setMaxPoolSize( config.MAX_CONN );
        cpds.setForceSynchronousCheckins( true );
        cpds.setIdleConnectionTestPeriod( 60 );
        cpds.setPreferredTestQuery( "select 1;" );
        for( int i = 0; i < config.MAX_CONN; ++i ) {
            cpds.getConnection().close();
        }

        biRepo = new BiRepo( cpds );
        sqlRepo = new SqlRepo( cpds );

        logger.info("init DataSource ok");

        return true;
    }

    public static BiRepo getBiRepo() {
        return biRepo;
    }

    public static SqlRepo getSqlRepo() {
        return sqlRepo;
    }

    public static void shutdown() {
        if( cpds != null ) cpds.close();
    }
}
