package com.dcx;/*
 */

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import static net.butfly.albacore.base.BizUnit.logger;

//import org.apache.log4j.Logger;

public class CqlTest2 {

//    public final static Logger logger = Logger.getLogger("CqlTest");
    public Session lastSession = null;
    public Cluster lastCluster = null;
    public int lastQueueSize = 100;
    public int lastBatchSize = 4096;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        new CqlTest2().test_1(args);
    }

    public void test_1(String[] args) {
        GetConn();
        String str0 = "DROP KEYSPACE IF EXISTS movies_new";
        ExecCql(str0, 1);
        String str1 = "CREATE KEYSPACE movies_new " +
                "WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor' : 3};";
        ExecCql(str1, 1);
        String str2 = "CREATE TABLE movies_new.nowshowing ( " +
                "movie TEXT," +
                "director TEXT static," +
                "main_actor TEXT static," +
                "released DATE static," +
                "location TEXT," +
                "run_day TEXT," +
                "run_time TIME," +
                "theater TEXT," +
                "PRIMARY KEY (movie, location, run_day, run_time)" +
                ");";
        ExecCql(str2, 1);
        String str3 = "INSERT INTO movies_new.nowshowing (movie, director, main_actor, released) " +
                "VALUES ('Sonic the Hedgehog', 'Jeff Fowler', 'Ben Schwartz', '2020-14-02') " +
                "IF NOT EXISTS;";
        ExecCql(str3, 1);
        String str4 = "INSERT INTO movies_new.nowshowing (movie, director, main_actor, released)" +
                "VALUES ('Invisible Man', 'Leigh Whannell', 'Elisabeth Moss', '2020-28-02')" +
                "IF NOT EXISTS;";
        ExecCql(str4, 1);

        String strx = "INSERT INTO movies_new.nowshowing (movie, director, main_actor, released,run_day,run_time,location)VALUES ('Sonic the Hedgehog', 'Leigh Whannell', 'Elisabeth Moss', '2020-28-02','Saturday','14:00:00','Times Square')IF NOT EXISTS;";
        ExecCql(strx, 1);

        String str5 = "UPDATE movies_new.nowshowing SET theater = 'AMC Empire'\n" +
                "WHERE location = 'Times Square'\n" +
                "  AND run_day = 'Saturday' AND run_time ='14:00:00'\n" +
                "  AND Movie = 'Sonic the Hedgehog'\n" +
                "IF EXISTS;";
        ExecCql(str5, 1);
        String str6 = "BEGIN BATCH\n" +
                "    DELETE FROM movies_new.nowshowing\n" +
                "    WHERE movie = 'Sonic the Hedgehog' AND location = 'Times Square'\n" +
                "      AND run_day = 'Saturday' AND run_time = '21:00:00'\n" +
                "    IF EXISTS\n" +
                "\n" +
                "    INSERT INTO movies_new.nowshowing (movie, location, theater, run_day, run_time)\n" +
                "    VALUES ('Sonic the Hedgehog', 'Times Square', 'AMC Empire 25', 'Saturday', '23:00:00')\n" +
                "APPLY BATCH;";
        ExecCql(str6, 1);
//        String str7 = "drop table movies_new.m2c_topic_qmj;";
        String str8 = "CREATE TABLE movies_new.m2c_topic_qmj (\n" +
                "    gmsfhm text,\n" +
                "    ci_src_db_table_name text,\n" +
                "    ci_update_time timestamp,\n" +
                "    csdssxq text,\n" +
                "    cssj text,\n" +
                "    etl_timestamp timestamp,\n" +
                "    priority int,\n" +
                "    PRIMARY KEY (gmsfhm)\n" +
                ") ;";
        String str9 = "CREATE INDEX m2c_topic_qmj_ci_update_time ON movies_new.m2c_topic_qmj (ci_update_time);";
        String str10 = "CREATE INDEX m2c_topic_qmj_etl_timestamp ON movies_new.m2c_topic_qmj (etl_timestamp);";
        String str11 = "CREATE INDEX m2c_topic_qmj_priority ON movies_new.m2c_topic_qmj (priority);";
//        ExecCql(str7, 1);
        ExecCql(str8, 1);
        ExecCql(str9, 1);
        ExecCql(str10, 1);
        ExecCql(str11, 1);
        String str20 = "BEGIN BATCH\n" +
                "insert into movies_new.m2c_topic_qmj (gmsfhm,cssj,csdssxq,priority,etl_timestamp) values ('500101198112206166','内蒙古鄂尔多斯','hdd',1,'2020-03-19T09:01:03.535+0000') IF NOT exists;\n" +
//                "UPDATE movies_new.m2c_topic_qmj SET csdssxq='yzx' WHERE gmsfhm='500101198112206166' IF priority < 2;\n" +
//                "UPDATE movies_new.m2c_topic_qmj SET csdssxq='dcx' WHERE gmsfhm='500101198112206166' IF etl_timestamp > '2020-01-08T09:54:21.261+0000';\n" +
                "APPLY BATCH;";
        ExecCql(str20, 1);
        String str21 = "BEGIN BATCH\n" +
//                "insert into movies_new.m2c_topic_qmj (gmsfhm,cssj,csdssxq,priority,etl_timestamp) values ('500101198112206166','内蒙古鄂尔多斯','hdd',1,'2020-03-19T09:01:03.535+0000') IF NOT exists;\n" +
                "UPDATE movies_new.m2c_topic_qmj SET csdssxq='yzx' WHERE gmsfhm='500101198112206166' IF priority < 2;\n" +
//                "UPDATE movies_new.m2c_topic_qmj SET csdssxq='dcx' WHERE gmsfhm='500101198112206166' IF etl_timestamp > '2020-01-08T09:54:21.261+0000';\n" +
                "APPLY BATCH;";
        ExecCql(str21, 1);
        String str22 = "BEGIN BATCH\n" +
//                "insert into movies_new.m2c_topic_qmj (gmsfhm,cssj,csdssxq,priority,etl_timestamp) values ('500101198112206166','内蒙古鄂尔多斯','hdd',1,'2020-03-19T09:01:03.535+0000') IF NOT exists;\n" +
//                "UPDATE movies_new.m2c_topic_qmj SET csdssxq='yzx' WHERE gmsfhm='500101198112206166' IF priority < 2;\n" +
                "UPDATE movies_new.m2c_topic_qmj SET csdssxq='dcx' WHERE gmsfhm='500101198112206166' IF etl_timestamp > '2020-01-08T09:54:21.261+0000';\n" +
                "APPLY BATCH;";
        ExecCql(str22, 1);
    }

    public Session GetConn() {
        if (lastSession != null) {
            return lastSession;
        }
        Session localSession = null;
        try {
            Cluster.Builder localBuilder = Cluster.builder();
            String localService = null;
            List<InetSocketAddress> localAddrList = new ArrayList<InetSocketAddress>();
            InetSocketAddress tmpAddr = new InetSocketAddress("10.19.120.98", 9042);
            localAddrList.add(tmpAddr);
//            tmpAddr = new InetSocketAddress("10.19.120.99", 9042);
//            localAddrList.add(tmpAddr);
//            tmpAddr = new InetSocketAddress("10.19.120.99", 9042);
//            localAddrList.add(tmpAddr);
            localBuilder.addContactPointsWithPorts(localAddrList);
            localBuilder.withRetryPolicy(DefaultRetryPolicy.INSTANCE);
            localBuilder.withQueryOptions(new QueryOptions()
                    .setConsistencyLevel(ConsistencyLevel.QUORUM) //.QUORUM
                    );
            localBuilder.withSocketOptions(new SocketOptions().setConnectTimeoutMillis(300000)
                .setReadTimeoutMillis(300000));
            lastCluster = localBuilder.build();
            if (localService == null) {
                localSession = lastCluster.connect();
            } else {
                localSession = lastCluster.connect(localService);
            }
            lastSession = localSession; //TODO connection init
            lastSession = lastSession.init();
            Configuration localConfig = lastCluster.getConfiguration();
            lastQueueSize = localConfig.getPoolingOptions().getMaxQueueSize();
        } catch (Throwable e) {
            logger.error("CassandraException", e);
            if (lastCluster != null) {
                lastCluster.close();
                lastCluster = null;
            }
        }
        return localSession;
    }

    public Session GetLastConn() {
        if (lastSession == null) {
            lastSession = GetConn();
        }
        return lastSession;
    }


    //执行SQL
    public int ExecCql(String sql, int paramLogFlag) {
        Session conn = null;
        int rc = 1;
        try {
            conn = GetLastConn();
            if (conn == null) {
                return -1;
            }
            ResultSet rs=conn.execute(sql);
            if(rs!=null)
            {
                List<ExecutionInfo> tmpInfoList=rs.getAllExecutionInfo();
                if(tmpInfoList!=null)
                {
                    for(ExecutionInfo tmpInfo:tmpInfoList)
                    {
                        if(tmpInfo.getWarnings()!=null)
                        {
                            logger.info(tmpInfo.getWarnings().toString());
                        }
                    }
                }
                rc = rs.getAvailableWithoutFetching();  //execute success
                if( rc > 0 )
                {
                    List <Row > tmpRowList=rs.all();
                    for(Row tmpRow:tmpRowList)
                    {
                        logger.info(tmpRow.toString());
                    }
                }
            }
            else
            {
                rc = 1;
            }
        } catch (Exception e) {
            if (paramLogFlag > 0) {
                logger.error("CassandraException", e);
            }
            rc = -1;
        } catch (Throwable e) {
            if (paramLogFlag > 0) {
                logger.error("CassandraException", e);
            }
            rc = -1;
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                    conn = null;
                    lastSession = null;
                }
                if (lastCluster != null) {
                    lastCluster.close();
                    lastCluster = null;
                }
            } catch (Throwable e1) {
                logger.error("CassandraSQLException", e1);
            }
        }
        logger.info(" exec :  " + sql + "  --- with result  " + rc);
        return rc;
    }
}