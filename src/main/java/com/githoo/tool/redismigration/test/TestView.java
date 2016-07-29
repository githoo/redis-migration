package com.githoo.tool.redismigration.test;

import com.githoo.tool.redismigration.conf.ConfigParseUtil;
import com.githoo.tool.redismigration.conf.ConfigUtil;
import com.githoo.tool.redismigration.config.RedisInfoConfigUtil;
import com.githoo.tool.redismigration.migration.RedisMigrationManager;
import com.githoo.tool.redismigration.redis.RedisKeyValue;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.util.*;


/**
 * Created with IntelliJ IDEA.
 * User: githoo
 * Date: 14-10-19
 * Time: 下午4:41
 * To change this template use File | Settings | File Templates.
 */
public class TestView {
    public static void main(String[] args) throws IOException, ParseException {

        List<String> skuList = new ArrayList<String>();
//        args=new String[]{"91761","97284","98274","90332"};
        if(args != null ){
            for(String str:args){
                System.out.println("key:"+str);
                skuList.add(str);
            }
        }
//        String sku  = "736617";
//        skuList.add(sku);
        List<byte[]> keys = new ArrayList<byte[]>();
        for(String id : skuList){
            keys.add(String.valueOf(id).getBytes());
        }
        Properties properties= ConfigParseUtil.parse(args, "redis-migration-" + ConfigUtil.conf + ".properties");
        ConfigParseUtil.printProperty(properties,120,"*");
        RedisInfoConfigUtil.init(properties);
        RedisMigrationManager.init();
//        testKeys(keys) ;
//        insertData(90000,10000);
//        insertData(50000,10000);
        testKeysKV(keys);
       /* List<String> skus =  RedisMigrationManager.scan(0,"90000");
        for(int i=0;i<skus.size();i++){
            System.out.println(i+"result : " +  skus.get(i) );
        }*/

    }

    public static void testKeysKV(List<byte[]> keys){
        List<RedisKeyValue>  r = RedisMigrationManager.getFromCacheKV(keys);
        for(int i=0;i<r.size();i++){
            try{
                RedisKeyValue t =  r.get(i);
                if(t!=null && !t.isEmpety()) {
                    System.out.println("result master key: " + new String( t.getKey()) + "  value: " + new String( t.getValue()) );
                }

            }catch (Exception e){
                System.out.println(i);
                e.printStackTrace();
            }
        }

        r = RedisMigrationManager.getFromCacheSlaverKV(keys);
        for(int i=0;i<r.size();i++){
            try{
                RedisKeyValue t =  r.get(i);
                if(t!=null && !t.isEmpety()) {
                    System.out.println("result slaver key: " + new String( t.getKey()) + "  value: " + new String( t.getValue()) );
                }
            }catch (Exception e){
                System.out.println(i);
                e.printStackTrace();
            }
        }
    }

    public static void testKeys(List<byte[]> keys){
        List<byte[]> r = RedisMigrationManager.getFromCache(keys);
        for(int i=0;i<r.size();i++){
            try{
                byte[] t =  r.get(i);
                System.out.println("result master: " + new String( r.get(i)) );
            }catch (Exception e){
                System.out.println(i);
                e.printStackTrace();
            }
        }

        r = RedisMigrationManager.getFromCacheSlaver(keys);
        for(int i=0;i<r.size();i++){
            try{
                byte[] t =  r.get(i);
                System.out.println("result slaver: " + new String( r.get(i)) );
            }catch (Exception e){
                System.out.println(i);
                e.printStackTrace();
            }
        }
    }
    public static void insertData(int start,int number){
        List<Long> skuList = new ArrayList<Long>();
        Map<byte[], byte[]> data = new HashMap<byte[], byte[]>();
        long sku  = start;

        for(int i=0;i<number;i++){
            String value =  String.valueOf(sku+i)+":value";
            data.put(String.valueOf(sku+i).getBytes(),value.getBytes()) ;
            skuList.add(sku+i);
        }
        long startTM = System.currentTimeMillis();
        RedisMigrationManager.setToCache(data, RedisInfoConfigUtil.expire);
        long latency = System.currentTimeMillis() - startTM;
        System.out.println("latency: "+latency);

       /* skuList.clear();
        for(int i=0;i<10;i++){
            skuList.add(sku+i);
        }
        List<byte[]> r = RedisMigrationManager.getFromCache(RedisMigrationManager.generateRedisKey(skuList));
        for(int i=0;i<r.size();i++){
            try{
              byte[] t =  r.get(i);
             System.out.println("result: " + new String( r.get(i)) );
            }catch (Exception e){
                System.out.println(i);
                e.printStackTrace();
            }
        }*/

//        List<String> skus =  RedisMigrationManager.scan(0,String.valueOf(sku));
//        for(int i=0;i<skus.size();i++){
//            System.out.println(i+"result : " +  skus.get(i) );
//        }

/*        RedisMigrationManager.migrationToCloud(generateRedisKey(skuList));
        RedisCacheCloud.mgetFromCache(skuList);*/

        //cloud
/*        long start = System.currentTimeMillis();
        RedisMigrationManager.migrationToCloudScan(0, String.valueOf(sku));
        long latency = System.currentTimeMillis() - start;
        System.out.println("latency: "+latency);*/
        //redis
    }
}
