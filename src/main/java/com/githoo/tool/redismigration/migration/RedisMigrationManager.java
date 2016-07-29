package com.githoo.tool.redismigration.migration;


import com.githoo.tool.redismigration.conf.ConfigParseUtil;
import com.githoo.tool.redismigration.conf.ConfigUtil;
import com.githoo.tool.redismigration.redis.RedisKeyValue;


import com.githoo.tool.redismigration.config.RedisInfoConfigUtil;

import com.githoo.tool.redismigration.redis.ShardedRedisCache;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.ShardedJedis;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: githoo
 * Date: 14-10-16
 * Time: 下午2:00
 * To change this template use File | Settings | File Templates.
 */
public class RedisMigrationManager {
    private static Log logger = LogFactory.getLog(RedisMigrationManager.class);
    private static ShardedRedisCache redisUitl ;
    private static ShardedRedisCache redisUitlSalver ;
    public static volatile AtomicLong  read_count = new AtomicLong(0) ;
    public static volatile AtomicLong  write_count = new AtomicLong(0) ;
    public static volatile AtomicLong  all_count = new AtomicLong(0) ;
    public static volatile AtomicLong  monitor_count = new AtomicLong(0) ;
    public static volatile AtomicLong  write_failure = new AtomicLong(0) ;
    public static final int retryTime = RedisInfoConfigUtil.retryWriteTime;
    public static final int tryExitTime = 3;

   /* static {
        redisUitl = new  ShardedRedisCache();
        redisUitl.init();
        redisUitlSalver = new ShardedRedisCache();
        redisUitlSalver.initSlaver();
    }*/


    public static void  init (){
        redisUitl = new  ShardedRedisCache();
        redisUitl.init();
        redisUitlSalver = new ShardedRedisCache();
        redisUitlSalver.initSlaver();
    }

    public static List<byte[]> getFromCache(List<byte[]> keys) {
        if (keys.isEmpty()) return new ArrayList<byte[]>();
        else return redisUitl.getFromCache(keys);
    }

    public static List<byte[]> getFromCacheSlaver(List<byte[]> keys) {
        if (keys.isEmpty()) return new ArrayList<byte[]>();
        else return redisUitlSalver.getFromCache(keys);
    }

    public static List<RedisKeyValue>  getFromCacheKV(List<byte[]> keys) {
        if (keys.isEmpty()) return new ArrayList<RedisKeyValue>();
        else return redisUitl.mgetKeyValue(keys);
    }

    public static List<RedisKeyValue>  getFromCacheSlaverKV(List<byte[]> keys) {
        if (keys.isEmpty()) return new ArrayList<RedisKeyValue>();
        else return redisUitlSalver.mgetKeyValue(keys);
    }


    public static void setToCache(Map<byte[], byte[]> data, int expire) {
        if (data.isEmpty()) return;
        redisUitl.setToCache(data,expire);
    }



    public static List<byte[]> generateRedisKey(List<Long> ids) {
        List<byte[]> keys = new ArrayList<byte[]>();
        for (Long id: ids) keys.add(String.valueOf(id).getBytes());
        return keys;
    }

    /**
     * 判断是否为空
     * @param obj 可以使普通对象、集合、map、String
     * @return empty:true  other:false
     */
    public static boolean isEmpty(Object obj) {
        boolean result = true;
        if (obj != null) {
            if (obj instanceof Collection) {
                result = ((Collection) obj).isEmpty();
            } else if (obj instanceof Map) {
                result = ((Map) obj).isEmpty();
            } else if (obj instanceof String) {
                result = "".equals(obj);
            } else {
                result = false;
            }
        }
        return result;
    }

    public static List<String> scan(int cursor,String key){
        return redisUitl.scan(cursor,key) ;
    }


    public static void migrationToCloud(List<byte[]> keys){
        List<RedisKeyValue> kvs = redisUitl.mgetKeyValue(keys);
//        RedisCacheCloud.msetToCache(kvs);
    }


    public static void migrationToSlaverScan(int cursor,String key){
        redisUitl.scanToSlaver(cursor, key);
    }

    public static void migrationToCloudScan(int cursor,String key){
        redisUitl.scanToCache(cursor, key);
    }

    public static void setSalverList(List<RedisKeyValue> kvs, int expire) {
        boolean flag = false;
        ShardedJedis shardedJedis = null;
        do{
            try {
                shardedJedis =  redisUitlSalver.getShardedJedis();
                if(shardedJedis != null) {
                    break;
                }
            }catch (Exception e){
               flag = true;
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }while (flag);

        for(RedisKeyValue kv :kvs ) {
            if (kv != null && !kv.isEmpety()) {
                RedisMigrationManager.write_count.incrementAndGet();
                boolean write_flag = false;
                int time = 0;
                do{
                    try{
                        redisUitlSalver.setValue(kv.getKey(),kv.getValue(),expire,shardedJedis);
                        write_flag = true;
                        break;
                    }catch (Exception e){
                        write_flag = flag;
                        if(time==0){
                            logger.error("write data failure,",e);
                            e.printStackTrace();
                        }
                        try {
                            redisUitlSalver.getShardedJedisPool().returnBrokenResource(shardedJedis);
                            Thread.sleep(5*time);
                        } catch (Exception e1) {
                            if(time==0)e1.printStackTrace();
                        }
                    }
                    time++;
                    System.out.println("######retryTime setSalverList:"+time);
                }while (!write_flag && time<RedisMigrationManager.retryTime);
                if(!write_flag) RedisMigrationManager.write_failure.incrementAndGet();
            }
        }
        redisUitlSalver.getShardedJedisPool().returnResource(shardedJedis);
//        System.out.println("######write count:"+RedisMigrationManager.write_count.get());
    }

    public static void main(String[] args) throws IOException, ParseException {

        Properties properties= ConfigParseUtil.parse(args, "redis-migration-" + ConfigUtil.conf + ".properties");
        ConfigParseUtil.printProperty(properties,120,"*");
        RedisInfoConfigUtil.init(properties);
        RedisMigrationManager.init();
        System.out.println("All########## start  migration ......");
       /* //定义线程池
        ExecutorService monitor = Executors.newFixedThreadPool(RedisInfoConfigUtil.masterthreadPoolsize);
        monitor.execute(new Runnable() {
            @Override
            public void run() {
                //To change body of implemented methods use File | Settings | File Templates.
                while (true){
                    if(isEnd.get()){
                        System.out.println("All########## start  migration ......");
                        System.out.println("######read count:"+RedisMigrationManager.read_count.get());
                        System.out.println("######write count:"+RedisMigrationManager.write_count.get());
                    }
                }
            }
        });
        monitor.shutdown();*/

        ExecutorService cachedThreadPool = Executors.newFixedThreadPool(RedisInfoConfigUtil.masterthreadPoolsize);
        String[] keys = RedisInfoConfigUtil.key.split(",");
        for(String keyTemp : keys){
            final  String key = keyTemp;
            //多线程
            cachedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    logger.error("##########key:"+key+", start ....");
                    System.out.println("##########key:"+key+", start ....");
                    long start = System.currentTimeMillis();
                    RedisMigrationManager.migrationToSlaverScan(0, key);
//                    RedisMigrationManager.migrationToCloudScan(0, key);
                    long latency = System.currentTimeMillis() - start;
                    System.out.println("Key:"+key+"##########latency: "+latency);
                    logger.error("Key:"+key+"##########latency: "+latency);
                    System.out.println("##########key:"+key+", end ....");
                    logger.error("##########key:"+key+", end ....");
                }
            });
        }
        cachedThreadPool.shutdown();
        System.out.println("All########## end  migration ......");
    }
}
