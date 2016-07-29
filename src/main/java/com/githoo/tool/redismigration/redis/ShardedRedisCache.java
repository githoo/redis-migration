package com.githoo.tool.redismigration.redis;


import com.githoo.tool.redismigration.config.RedisInfoConfigUtil;
import com.githoo.tool.redismigration.migration.RedisMigrationManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.*;


import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ShardedRedisCache {
    private static Log logger = LogFactory.getLog(ShardedRedisCache.class);

    private JedisPoolConfig jedisPoolConfig;
    private String hosts;
    private ShardedJedisPool shardedJedisPool;
    private int timeout;
    private int database;
    private int expire;
    public Map<String,String> master_slaver_relation ;


    public void init() {
        //jedisPoolconfig
        jedisPoolConfig = new  JedisPoolConfig();

       // jedisPoolConfig.setMaxActive(JedisPoolConfigUitl.maxActive);
        jedisPoolConfig.setMaxIdle(RedisInfoConfigUtil.maxIdle);
        jedisPoolConfig.setMinIdle(RedisInfoConfigUtil.minIdle);
        //jedisPoolConfig.setMaxWait(JedisPoolConfigUitl.maxWait);
        jedisPoolConfig.setMaxWaitMillis(RedisInfoConfigUtil.maxWait);
        //redis  info config
        hosts = RedisInfoConfigUtil.hosts;
        timeout = RedisInfoConfigUtil.timeout;
        database = RedisInfoConfigUtil.database;
        expire = RedisInfoConfigUtil.expire;

        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        String[] hostAndPorts = hosts.split(" ");
        List<String> hostList = new ArrayList<String>();
        for (String hostAndPort: hostAndPorts) hostList.add(hostAndPort);
        for (String hostAndPort : hostList) {
            int pos = hostAndPort.indexOf(":");
            if(pos > 0) {
                JedisShardInfo jedis = new JedisShardInfo(hostAndPort.substring(0, pos), Integer.valueOf(hostAndPort.substring(pos + 1)), timeout);
                shards.add(jedis);
            }
        }
        shardedJedisPool = new ShardedJedisPool(jedisPoolConfig, shards);
    }

    public void initSlaver() {
        //jedisPoolconfig
        jedisPoolConfig = new  JedisPoolConfig();

        // jedisPoolConfig.setMaxActive(JedisPoolConfigUitl.maxActive);
        jedisPoolConfig.setMaxIdle(RedisInfoConfigUtil.maxIdle);
        jedisPoolConfig.setMinIdle(RedisInfoConfigUtil.minIdle);
        //jedisPoolConfig.setMaxWait(JedisPoolConfigUitl.maxWait);
        jedisPoolConfig.setMaxWaitMillis(RedisInfoConfigUtil.maxWait);
        //redis  info config
        hosts = RedisInfoConfigUtil.hosts_slaver;
        timeout = RedisInfoConfigUtil.timeout_slaver;
        database = RedisInfoConfigUtil.database_slaver;
        expire = RedisInfoConfigUtil.expire_slaver;

        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        String[] hostAndPorts = hosts.split(" ");
//        Set<String> stringSet = new HashSet<String>();
        List<String> hostList = new ArrayList<String>();
        for (String hostAndPort: hostAndPorts) hostList.add(hostAndPort);

        for (String hostAndPort : hostList) {
            int pos = hostAndPort.indexOf(":");
            if(pos > 0) {
                JedisShardInfo jedis = new JedisShardInfo(hostAndPort.substring(0, pos), Integer.valueOf(hostAndPort.substring(pos + 1)), timeout);
                shards.add(jedis);
            }
        }

        shardedJedisPool = new ShardedJedisPool(jedisPoolConfig, shards);

    }


    public ShardedJedis getResource() {
        return shardedJedisPool.getResource();
    }

    public void returnResource(ShardedJedis jedis) {
        shardedJedisPool.returnResource(jedis);
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public int getDatabase(){
        return this.database;
    }


    public  void scanToCache(int cursor,String key) {
        scanToSave(cursor,key,true) ;
    }

    public  void scanToSlaver(int cursor,String key) {
        scanToSave(cursor,key,false);
     /*   final ShardedJedis shardedJedis = shardedJedisPool.getResource();
        final Jedis jedis = shardedJedis.getShard(key);
        int count = 0;
        //定义线程池
        ExecutorService cachedThreadPool = Executors.newFixedThreadPool(RedisInfoConfigUtil.threadPoolSize);
        do{
            ScanResult<String> rs = jedis.scan(cursor)  ;
            List<String> skus = new ArrayList<String>() ;
            skus.addAll(rs.getResult());
            final List<byte[]> keys = new ArrayList<byte[]>();
            for(int i=0;i<skus.size();i++){
                keys.add(skus.get(i).getBytes());
            }
//            System.out.println("########## start execute count:  "+ (count+1));
            count += keys.size();
            RedisMigrationManager.all_count.addAndGet(keys.size()) ;
            //多线程
            cachedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    List<RedisKeyValue> kvs = mgetKeyValueList(keys);
                    //RedisCacheCloud.msetToCache(kvs);
                    RedisMigrationManager.setSalverList(kvs, expire);
                }
            });
            System.out.println("##########  complete count:  "+count);
            cursor = rs.getCursor();
        }while(cursor > 0)  ;
        cachedThreadPool.shutdown();
        shardedJedisPool.returnResource(shardedJedis);
        System.out.println("##########count:  "+count);
        logger.error("##########count:  "+count);
        logger.error("##########all_count: "+RedisMigrationManager.all_count.get());*/
    }

    public  void scanToSave( int cursor,String key, final boolean isCloud) {
        final ShardedJedis shardedJedis = shardedJedisPool.getResource();
        final Jedis jedis = shardedJedis.getShard(key);
        int count = 0;
        //定义线程池
        ExecutorService cachedThreadPool = Executors.newFixedThreadPool(RedisInfoConfigUtil.threadPoolSize);
        do{
            final ScanResult<String> rs = jedis.scan(cursor)  ;
            List<String> skus = new ArrayList<String>() ;
            skus.addAll(rs.getResult());
            final List<byte[]> keys = new ArrayList<byte[]>();
            for(int i=0;i<skus.size();i++){
                keys.add(skus.get(i).getBytes());
            }
//            System.out.println("########## start execute count:  "+ (count+1));
            count += keys.size();
            RedisMigrationManager.all_count.addAndGet(keys.size()) ;
            //多线程
            cachedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    List<RedisKeyValue> kvs = mgetKeyValueList(keys);
//                    if(isCloud){
//                        RedisCacheCloud.msetToCache(kvs);
//                    } else{
                        RedisMigrationManager.setSalverList(kvs, expire);
//                    }
                }
            });
//            System.out.println("##########  complete count:  " + count);
            cursor = rs.getCursor();
        }while(cursor > 0)  ;
        cachedThreadPool.shutdown();
        shardedJedisPool.returnResource(shardedJedis);
        System.out.println("##########count:  "+count);
        logger.error("##########count:  "+count);
        logger.error("##########all_count: "+RedisMigrationManager.all_count.get());
    }


    public  List<String> scan(int cursor,String key) {
        List<String> skus = new ArrayList<String>() ;
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        Jedis jedis = shardedJedis.getShard(key);
        do{
             ScanResult<String> rs = jedis.scan(cursor)  ;
             skus.addAll(rs.getResult());
             cursor = rs.getCursor();
        }while(cursor > 0)  ;
        return skus;
    }





    public  byte[] getValue(byte[] key){
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        Jedis jedis = shardedJedis.getShard(key);
        byte[]  r = jedis.get(key);
        shardedJedisPool.returnResource(shardedJedis);
        return r ;
    }

    public ShardedJedis getShardedJedis(){
        return shardedJedisPool.getResource();
    }

    public  ShardedJedisPool getShardedJedisPool(){
          return shardedJedisPool;
    }

    public  void setValue(byte[] key,byte[] value,int expire, ShardedJedis shardedJedis ){
        Jedis jedis = shardedJedis.getShard(key);
        jedis.setex(key, expire,value);
        //shardedJedisPool.returnResource(shardedJedis);
    }

    public List<byte[]> getFromCache(List<byte[]> keys) {
        if (keys.isEmpty()) return new ArrayList<byte[]>();
        else return mget(keys);
    }






    private List<byte[]> mget(List<byte[]> keys) {
        List<byte[]> result = new ArrayList<byte[]>(keys.size());
        for (int i=0; i<keys.size(); i++) result.add(null);
        ShardedJedis shardedJedis = shardedJedisPool.getResource();

        try {
            Map<Jedis, List<byte[]>> keysMap = new HashMap<Jedis, List<byte[]>>();
            Map<Jedis, List<Integer>> indexMap = new HashMap<Jedis, List<Integer>>();
            for (int i=0; i< keys.size(); i++) {
                byte[] key = keys.get(i);

                Jedis jedis = shardedJedis.getShard(key);
                List<byte[]> jKeys = keysMap.get(jedis);
                if (jKeys == null) {
                    jKeys = new ArrayList<byte[]>();
                    keysMap.put(jedis, jKeys);
                }
                jKeys.add(key);

                List<Integer> index = indexMap.get(jedis);
                if (index == null) {
                    index = new ArrayList<Integer>();
                    indexMap.put(jedis, index);
                }
                index.add(i);
            }

            for(Map.Entry<Jedis, List<byte[]>> keyEntry: keysMap.entrySet()){
                Jedis jedis = keyEntry.getKey();
                List<byte[]> jKeys = keyEntry.getValue();
                List<Integer> index = indexMap.get(jedis);

                byte[][] queryKeys = new byte[jKeys.size()][];
                jKeys.toArray(queryKeys);

                List<byte[]> values;
                if (database == 0){
                    values = jedis.mget(queryKeys);
                } else {
                    Pipeline p = jedis.pipelined();
                    p.select(database);
                    Response<List<byte[]>> valuesResponse = p.mget(queryKeys);
                    p.sync();
                    values = valuesResponse.get();
                }

                for (int i=0; i<jKeys.size(); i++) {
                    result.set(index.get(i), values.get(i));
                }
            }
            shardedJedisPool.returnResource(shardedJedis);
        } catch (Exception e){
            logger.error("error in redis get", e);
            shardedJedisPool.returnBrokenResource(shardedJedis);
        }

        return result;
    }

    public  void setToCache(byte[] key, byte[] value, int expire) {

    }

    public  void setToCache(Map<byte[], byte[]> data, int expire) {
        if (data.isEmpty()) return;
        msetex(data, expire);
    }

    private void msetex(Map<byte[], byte[]> keyValuePairs, int expire) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        if (shardedJedis == null) return;
        try {
            Map<Jedis, Map<byte[], byte[]>> jedisMap = new HashMap<Jedis, Map<byte[], byte[]>>();
            for (Map.Entry<byte[], byte[]> pair: keyValuePairs.entrySet()) {
                byte[] key = pair.getKey();
                byte[] value = pair.getValue();

                Jedis jedis = shardedJedis.getShard(pair.getKey());
                Map<byte[], byte[]> kvPair = jedisMap.get(jedis);
                if (kvPair == null) {
                    kvPair = new HashMap<byte[], byte[]>();
                    jedisMap.put(jedis, kvPair);
                }

                kvPair.put(key, value);
            }

            for (Map.Entry<Jedis, Map<byte[], byte[]>> entry: jedisMap.entrySet()) {
                Jedis jedis = entry.getKey();
                Map<byte[], byte[]> kvPair = entry.getValue();

                if (database == 0){
                    for (Map.Entry<byte[], byte[]> kvPairEntry: kvPair.entrySet()){
                        jedis.setex(kvPairEntry.getKey(), expire, kvPairEntry.getValue());
                    }
                } else {
                    Pipeline p = jedis.pipelined();
                    p.select(database);
                    for (Map.Entry<byte[], byte[]> kvPairEntry: kvPair.entrySet()){
                        p.setex(kvPairEntry.getKey(), expire, kvPairEntry.getValue());
                    }
                    p.sync();
                }
            }

            shardedJedisPool.returnResource(shardedJedis);
        } catch (Exception e) {
            logger.error("error in redis mset", e);
            shardedJedisPool.returnBrokenResource(shardedJedis);
        }
    }



    public List<RedisKeyValue> mgetKeyValue(List<byte[]> keys) {
         List<RedisKeyValue> result = new ArrayList<RedisKeyValue>(keys.size());
         for (int i=0; i<keys.size(); i++) result.add(null);
         ShardedJedis shardedJedis = shardedJedisPool.getResource();

         try {
             Map<Jedis, List<byte[]>> keysMap = new HashMap<Jedis, List<byte[]>>();
             Map<Jedis, List<Integer>> indexMap = new HashMap<Jedis, List<Integer>>();
//             System.out.println("query-key-size:" + keys.size());
             for (int i=0; i< keys.size(); i++) {
                 byte[] key = keys.get(i);

                 Jedis jedis = shardedJedis.getShard(key);
                 List<byte[]> jKeys = keysMap.get(jedis);
                 if (jKeys == null) {
                     jKeys = new ArrayList<byte[]>();
                     keysMap.put(jedis, jKeys);
                 }
                 jKeys.add(key);

                 List<Integer> index = indexMap.get(jedis);
                 if (index == null) {
                     index = new ArrayList<Integer>();
                     indexMap.put(jedis, index);
                 }
                 index.add(i);
             }

//             System.out.println("query keysMap:"+keysMap.size()+"content: "+keysMap);
             for(Map.Entry<Jedis, List<byte[]>> keyEntry: keysMap.entrySet()){
                 Jedis jedis = keyEntry.getKey();
                 List<byte[]> jKeys = keyEntry.getValue();
                 List<Integer> index = indexMap.get(jedis);

                 byte[][] queryKeys = new byte[jKeys.size()][];
                 jKeys.toArray(queryKeys);

                 List<byte[]> values;
                 if (database == 0){
                     values = jedis.mget(queryKeys);
                 } else {
                     Pipeline p = jedis.pipelined();
                     p.select(database);
                     Response<List<byte[]>> valuesResponse = p.mget(queryKeys);
                     p.sync();
                     values = valuesResponse.get();
                 }

                 for (int i=0; i<jKeys.size(); i++) {
                     RedisKeyValue kv = new RedisKeyValue();
                     kv.setKey(keys.get(i));
                     kv.setValue(values.get(i));
                     kv.setEmpety(values.get(i)==null?true:false);
                     if(!kv.isEmpety()) RedisMigrationManager.read_count.incrementAndGet();
                     result.set(index.get(i), kv);
//                     result.set(index.get(i), values.get(i));
                 }
             }
             shardedJedisPool.returnResource(shardedJedis);
             System.out.println("######read count:"+RedisMigrationManager.read_count.get());
         } catch (Exception e){
             logger.error("error in redis get", e);
             shardedJedisPool.returnBrokenResource(shardedJedis);
         }
         return result;
     }

    public List<RedisKeyValue> mgetKeyValueList(List<byte[]> keys) {
        List<RedisKeyValue> result = new ArrayList<RedisKeyValue>(keys.size());
        for (int i=0; i<keys.size(); i++) result.add(null);
        boolean flag = false;
        ShardedJedis shardedJedis = null;
        do{
            try {
                shardedJedis = shardedJedisPool.getResource();
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
        boolean read_flag = false;
        int time = 0;
        do{
            try {
                Map<Jedis, List<byte[]>> keysMap = new HashMap<Jedis, List<byte[]>>();
                Map<Jedis, List<Integer>> indexMap = new HashMap<Jedis, List<Integer>>();
                for (int i=0; i< keys.size(); i++) {
                    byte[] key = keys.get(i);

                    Jedis jedis = shardedJedis.getShard(key);
                    List<byte[]> jKeys = keysMap.get(jedis);
                    if (jKeys == null) {
                        jKeys = new ArrayList<byte[]>();
                        keysMap.put(jedis, jKeys);
                    }
                    jKeys.add(key);

                    List<Integer> index = indexMap.get(jedis);
                    if (index == null) {
                        index = new ArrayList<Integer>();
                        indexMap.put(jedis, index);
                    }
                    index.add(i);
                }

                for(Map.Entry<Jedis, List<byte[]>> keyEntry: keysMap.entrySet()){
                    Jedis jedis = keyEntry.getKey();
                    List<byte[]> jKeys = keyEntry.getValue();
                    List<Integer> index = indexMap.get(jedis);

                    byte[][] queryKeys = new byte[jKeys.size()][];
                    jKeys.toArray(queryKeys);

                    List<byte[]> values;
                    if (database == 0){
                        values = jedis.mget(queryKeys);
                    } else {
                        Pipeline p = jedis.pipelined();
                        p.select(database);
                        Response<List<byte[]>> valuesResponse = p.mget(queryKeys);
                        p.sync();
                        values = valuesResponse.get();
                    }

                    for (int i=0; i<jKeys.size(); i++) {
                        RedisKeyValue kv = new RedisKeyValue();
                        kv.setKey(keys.get(i));
                        kv.setValue(values.get(i));
                        kv.setEmpety(values.get(i)==null?true:false);
                        if(!kv.isEmpety()) RedisMigrationManager.read_count.incrementAndGet();
                        result.set(index.get(i), kv);
    //                     result.set(index.get(i), values.get(i));
                    }
                }
                shardedJedisPool.returnResource(shardedJedis);
//                System.out.println("######read count:"+RedisMigrationManager.read_count.get());
//                read_flag = true;
                break;
            } catch (Exception e){
                read_flag = false;
                if(time==0){
                    logger.error("error in redis get", e);
                    e.printStackTrace();
                }
                try {
                    shardedJedisPool.returnBrokenResource(shardedJedis);
                    Thread.sleep(5*time);
                } catch (Exception e1) {
                    if(time==0)e1.printStackTrace();
                }
            }
            time++;
            System.out.println("######retryTime mgetKeyValueList:"+time);
        }while (!read_flag && time < RedisMigrationManager.retryTime);
        return result;
    }


    public void msetexRedisKeyValue(List<RedisKeyValue> keyValuePairs, int expire) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        if (shardedJedis == null) return;
        try {
            Map<Jedis, Map<byte[], byte[]>> jedisMap = new HashMap<Jedis, Map<byte[], byte[]>>();
            for (RedisKeyValue pair: keyValuePairs) {
                if(!pair.isEmpety()) {
                    byte[] key = pair.getKey();
                    byte[] value = pair.getValue();
                    Jedis jedis = shardedJedis.getShard(pair.getKey());
                    Map<byte[], byte[]> kvPair = jedisMap.get(jedis);
                    if (kvPair == null) {
                        kvPair = new HashMap<byte[], byte[]>();
                        jedisMap.put(jedis, kvPair);
                    }
                    kvPair.put(key, value);
                }
            }

            for (Map.Entry<Jedis, Map<byte[], byte[]>> entry: jedisMap.entrySet()) {
                Jedis jedis = entry.getKey();
                Map<byte[], byte[]> kvPair = entry.getValue();

                if (database == 0){
                    for (Map.Entry<byte[], byte[]> kvPairEntry: kvPair.entrySet()){
                        jedis.setex(kvPairEntry.getKey(), expire, kvPairEntry.getValue());
                    }
                } else {
                    Pipeline p = jedis.pipelined();
                    p.select(database);
                    for (Map.Entry<byte[], byte[]> kvPairEntry: kvPair.entrySet()){
                        p.setex(kvPairEntry.getKey(), expire, kvPairEntry.getValue());
                    }
                    p.sync();
                }
            }

            shardedJedisPool.returnResource(shardedJedis);
        } catch (Exception e) {
            logger.error("error in redis mset", e);
            shardedJedisPool.returnBrokenResource(shardedJedis);
        }
    }

    public static void main(String[] args) {
        String hashKey = "hash_key";

 /*       // HDEL
        jedis.del(hashKey);

        // HGETALL
        System.out.println("0--------" + jedis.hgetAll(hashKey));

        // HEXISTS
        System.out.println("BEFORE--" + jedis.hexists(hashKey, "NAME"));

        // HSET
        jedis.hset(hashKey, "NAME", "SHIVAGANESH");
        System.out.println("1--------" + jedis.hgetAll(hashKey));

        // HEXISTS
        System.out.println("AFTER--" + jedis.hexists(hashKey, "NAME"));

        // HMSET
        HashMap<String, String> multiMap = new HashMap<String, String>();
        multiMap.put("KEY 1", "VALUE 1");
        multiMap.put("KEY 2", "VALUE 2");
        jedis.hmset(hashKey, multiMap);
        System.out.println("2--------" + jedis.hgetAll(hashKey));

        // HMGET
        String[] fields = { "KEY 1", "KEY 0", "KEY 2", "KEY 3" };
        List<String> keys = jedis.hmget(hashKey, fields);
        System.out.println("KEYS -----" + keys);

        // HINCRBY
        // HGET
        System.out
                .println("HINCRBY---BEFORE--" + jedis.hget(hashKey, "VISITS"));
        jedis.hincrBy(hashKey, "VISITS", 1);
        System.out.println("HINCRBY---AFTER--" + jedis.hget(hashKey, "VISITS"));

        // HINCRBYFLOAD
        System.out.println("HINCRBYFLOAT---BEFORE--"
                + jedis.hget(hashKey, "VISITS"));
        jedis.hincrByFloat(hashKey, "VISITS", 1.11);
        System.out.println("HINCRBYFLOAT---AFTER--"
                + jedis.hget(hashKey, "VISITS"));

        // HKEYS
        System.out.println("HASHKEYS -----" + jedis.hkeys(hashKey));

        // HVALUES
        System.out.println("HASHVALUES -----" + jedis.hvals(hashKey));

        // HLEN
        System.out.println("HASH LEN -----" + jedis.hlen(hashKey));

        // HSETNX
        jedis.hsetnx(hashKey, "NEW KEY", "NEW VALUE");
        System.out.println("HSETNX---BEFORE--" + jedis.hgetAll(hashKey));
        jedis.hsetnx(hashKey, "NEW KEY", "NEW VALUE 2");
        System.out.println("HSETNX---AFTER--" + jedis.hgetAll(hashKey));

        // HSCAN
        System.out.println("\n\n\n----------HSCAN--------------------");
        jedis.del(hashKey);
        for (int i = 0; i < 1000; i++) {
            jedis.hset(hashKey, "USERNAME" + i, "PASSWORD" + i);
        }
        ScanParams params = new ScanParams();
        params.match("*");
        boolean scanningDone = false;
        String start = SCAN_POINTER_START;
        while (!scanningDone) {
            ScanResult<Entry<String, String>> scanResults = jedis
                    .hscan(hashKey, start, params);
            for (Entry<String, String> eachScanResult : scanResults.getResult()) {
                System.out.println(eachScanResult);
            }
            start = scanResults.getStringCursor();
            if (start.equalsIgnoreCase("0")) {
                scanningDone = true;
            }
            System.out.println(scanResults.getStringCursor());
        }*/
    }
}
