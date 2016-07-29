package com.githoo.tool.redismigration.test;

import com.githoo.tool.redismigration.migration.RedisMigrationManager;
import org.apache.commons.cli.ParseException;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: githoo
 * Date: 14-10-21
 * Time: 上午10:07
 * To change this template use File | Settings | File Templates.
 */
public class Test {
    public static void main(String[] args) {
        String[] aa={"-p","D:\\study\\redis-migration\\src\\main\\resources\\redis-migration-test.properties"};
        try {
            RedisMigrationManager.main(aa);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

}
