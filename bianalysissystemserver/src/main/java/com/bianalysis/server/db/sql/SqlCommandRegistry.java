package com.bianalysis.server.db.sql;


import com.bianalysis.server.utils.PropUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.SAXParserFactory;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SqlCommandRegistry {

    private static final Logger logger = LoggerFactory.getLogger(SqlCommandRegistry.class);

    private static final SqlCommandRegistry INSTANCE = new SqlCommandRegistry();

    private Map< String, SqlCommand > mapping = new HashMap<>();

    private SqlCommandRegistry() {
    }

    public static SqlCommandRegistry getInstance() {
        return INSTANCE;
    }

    public SqlCommand get( String key ) {
        return mapping.get( key );
    }

    public SqlCommand put( String key, SqlCommand val ) {
        return mapping.put( key, val );
    }

    /**
     * xml 初始化
     * @param filePath
     * @throws Exception
     *
     * 自定义扩展自DefaultHandler的子类，覆写一些解析XML时我们需要的方法
     */
    public void init( String filePath ) throws Exception {

        SAXParserFactory.newInstance().newSAXParser()
                .parse( PropUtil.getInputStream( filePath ), new SqlCommandLoader() );

        for( SqlCommand cmd : mapping.values() ) {
            cmd.init();
        }

        logger.info("SqlCommand init ok");
    }

    public void end() throws SQLException {
        for( SqlCommand cmd : mapping.values() ) {
            cmd.end();
        }
    }

}
