package com.bianalysis.server.sql;


import com.bianalysis.server.utils.PropUtil;

import javax.xml.parsers.SAXParserFactory;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SqlCommandRegistry {

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
    }

    public void end() throws SQLException {
        for( SqlCommand cmd : mapping.values() ) {
            cmd.end();
        }
    }

}