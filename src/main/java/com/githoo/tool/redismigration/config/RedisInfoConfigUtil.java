package com.githoo.tool.redismigration.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: githoo
 * Date: 14-10-16
 * Time: 下午2:28
 * To change this template use File | Settings | File Templates.
 */
public class RedisInfoConfigUtil {
    private static final Log logger = LogFactory.getLog(RedisInfoConfigUtil.class);
    public static String  hosts ;
    public static  int  database = 0 ;
    public static  int  timeout = 0;
    public static  int expire = 864000;
    public static  String key = "10000";

    public static  String  hosts_slaver = "192.123.195.178:6379";
    public static  int  database_slaver = 0 ;
    public static  int  timeout_slaver = 0;
    public static  int expire_slaver = 864000;
    public static  String key_slaver = "10000";
    public static  int masterthreadPoolsize = 9;

    public static   int  maxActive = 100;
    public static   int  maxIdle = 50 ;
    public static   int  minIdle = 20;
    public static   long maxWait = 2000;

    public static   int  threadPoolSize = 240;



    public static boolean isCloud =false;

    public static   int   retryWriteTime = 3;
    /*static {

        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("redis-migration-"+ ConfigUtil.conf+".properties");
        try {
            Properties properties = new Properties();
            properties.load(in);
            hosts = properties.getProperty("master.hosts");
            database = Integer.parseInt(properties.getProperty("master.database"));
            timeout = Integer.parseInt(properties.getProperty("master.timeout"));
            expire = Integer.parseInt(properties.getProperty("master.expire"));
            key = properties.getProperty("master.key");
            masterthreadPoolsize = Integer.parseInt(properties.getProperty("master.threadPoolSize"));

            hosts_slaver = properties.getProperty("slaver.hosts");
            database_slaver = Integer.parseInt(properties.getProperty("slaver.database"));
            timeout_slaver = Integer.parseInt(properties.getProperty("slaver.timeout"));
            expire_slaver = Integer.parseInt(properties.getProperty("slaver.expire"));
            key_slaver = properties.getProperty("slaver.key");

            maxActive = Integer.parseInt(properties.getProperty("JedisPoolConfig.maxActive"));
            maxIdle = Integer.parseInt(properties.getProperty("JedisPoolConfig.maxIdle"));
            minIdle = Integer.parseInt(properties.getProperty("JedisPoolConfig.minIdle"));
            maxWait = Integer.parseInt(properties.getProperty("JedisPoolConfig.maxWait"));

            threadPoolSize = Integer.parseInt(properties.getProperty("threadPoolSize"));

            zkServerPath = properties.getProperty("zkServerPath");
            zkCacheCloudPath = properties.getProperty("zkCacheCloudPath");

        } catch (IOException e) {
            logger.error("properties is not found", e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                logger.error("load properties failed", e);
            }
        }
    }*/

    public static void init(Properties properties){
        hosts = properties.getProperty("master.hosts");
        database = Integer.parseInt(properties.getProperty("master.database"));
        timeout = Integer.parseInt(properties.getProperty("master.timeout"));
        expire = Integer.parseInt(properties.getProperty("master.expire"));
        key = properties.getProperty("master.key");
        masterthreadPoolsize = Integer.parseInt(properties.getProperty("master.threadPoolSize"));

        hosts_slaver = properties.getProperty("slaver.hosts");
        database_slaver = Integer.parseInt(properties.getProperty("slaver.database"));
        timeout_slaver = Integer.parseInt(properties.getProperty("slaver.timeout"));
        expire_slaver = Integer.parseInt(properties.getProperty("slaver.expire"));
        key_slaver = properties.getProperty("slaver.key");

        maxActive = Integer.parseInt(properties.getProperty("JedisPoolConfig.maxActive"));
        maxIdle = Integer.parseInt(properties.getProperty("JedisPoolConfig.maxIdle"));
        minIdle = Integer.parseInt(properties.getProperty("JedisPoolConfig.minIdle"));
        maxWait = Integer.parseInt(properties.getProperty("JedisPoolConfig.maxWait"));

        threadPoolSize = Integer.parseInt(properties.getProperty("threadPoolSize"));



        isCloud = Boolean.parseBoolean(properties.getProperty("isCloud"));
        retryWriteTime  = Integer.parseInt(properties.getProperty("retryWriteTime"));
    }
}

