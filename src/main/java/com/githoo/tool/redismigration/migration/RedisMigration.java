package com.githoo.tool.redismigration.migration;

import com.githoo.tool.redismigration.conf.ConfigParseUtil;
import com.githoo.tool.redismigration.conf.ConfigUtil;
import com.githoo.tool.redismigration.config.RedisInfoConfigUtil;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: githoo
 * Date: 14-10-21
 * Time: 下午4:29
 * To change this template use File | Settings | File Templates.
 */
public class RedisMigration {
    private static Log logger = LogFactory.getLog(RedisMigration.class);

    public static void main(String[] args) throws IOException, ParseException {

        Properties properties= ConfigParseUtil.parse(args, "redis-migration-" + ConfigUtil.conf + ".properties");
        ConfigParseUtil.printProperty(properties,120,"*");
        RedisInfoConfigUtil.init(properties);
        RedisMigrationManager.init();
        ScheduledExecutorService  monitor = Executors.newScheduledThreadPool(1);
        monitor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
               logger.error("monitor##########all_count: "+RedisMigrationManager.all_count.get());
               logger.error("monitor##########read_count: "+RedisMigrationManager.read_count.get());
                logger.error("monitor##########write_failure: "+RedisMigrationManager.write_failure.get());
               long writeTotal =  RedisMigrationManager.write_count.get();
               logger.error("monitor##########write_count: "+writeTotal);
                try {
                    Thread.sleep(1000);
                    if(writeTotal == RedisMigrationManager.write_count.get()) {
                        if(RedisMigrationManager.tryExitTime <= RedisMigrationManager.monitor_count.incrementAndGet() ){
                            logger.error("monitor##########write_count: "+RedisMigrationManager.write_count.get()+" is continuous "
                                    +RedisMigrationManager.tryExitTime+" time no change, so exit....");
                            System.out.println("monitor##########write_count: "+RedisMigrationManager.write_count.get()+" is continuous "
                                    +RedisMigrationManager.tryExitTime+" time no change, so exit....");
                            System.exit(0);
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }, 10, 300,TimeUnit.SECONDS);


        System.out.println("All##########isCloud: "+RedisInfoConfigUtil.isCloud+" start  migration ......");
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
                    if(RedisInfoConfigUtil.isCloud){
                        RedisMigrationManager.migrationToCloudScan(0, key);
                    }else{
                        RedisMigrationManager.migrationToSlaverScan(0, key);
                    }
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
