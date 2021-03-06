package com.bianalysis.server.db.sql;

import com.bianalysis.server.db.repo.RepoManager;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class SqlCommand {

    /**
     * sql 命令
     */
    public String sql;
    public String batchSql;
    /**
     * 参数模式 一定要和 sql语句中的列数对应上
     */
    public String paramPattern;
    /**
     * 问号的个数
     */
    public int paramCount;
    /**
     * 服务器在缓冲多少条以后集中向数据库发送一次请求
     */
    public int batchCount;
    /**
     * values list (支持多条数据发送)
     */
    public List< String[] > values;

    public SqlCommand() {
        values = new LinkedList< String[] >();
    }

    /**
     * 初始化
     *
     * @return
     */
    public boolean init() {
        if( batchCount < 1 ) return false;
        StringBuilder sb = new StringBuilder();
        sb.append( sql );
        for(int i = 0; i < batchCount; ++i ) {
            sb.append( paramPattern ).append( i == batchCount - 1 ? ";" : "," );
        }
        batchSql = sb.toString();
        sql = sql + paramPattern + ";";
        return true;
    }

    /**
     * 对当前命令添加一条数据
     * @param val
     * @return
     * @throws SQLException
     */
    public boolean push(String[] val ) throws SQLException {
        if( val.length != paramCount )
            return false;

        List< String[] > tmp = null;
        boolean ret = true;
        synchronized( this ) {
            values.add( val );
            if( values.size() == batchCount ) {
                tmp = values;
                values = new LinkedList< String[] >();
            }
        }
        if( tmp != null ) {
            // batch的方式发送
            ret = RepoManager.getBiRepo().batchUpdate( batchSql, tmp );
        }
        return ret;
    }

    /**
     * 因为命令是以batch的方式发送，可能会导致退出时还有一些命令有堆积的数据没有发送
     * 那么先将其发送
     *
     * @return
     * @throws SQLException
     */
    public boolean end() throws SQLException {
        List< String[] > tmp = null;
        synchronized( this ) {
            tmp = values;
            values = new LinkedList< String[] >();
        }
        if( tmp != null ) {
            for( String[] x : tmp ) {
                RepoManager.getBiRepo().update( sql, x );
            }
        }
        return true;
    }
}
