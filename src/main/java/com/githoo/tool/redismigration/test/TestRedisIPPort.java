package com.githoo.tool.redismigration.test;



import java.util.HashMap;
import java.util.HashSet;

import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: githoo
 * Date: 14-10-23
 * Time: 上午10:24
 * To change this template use File | Settings | File Templates.
 */
public class TestRedisIPPort {
    public static void main(String[] args) {
        Map<String,String> master_slaver_relation = new HashMap<String, String>();
        String key_port = "6403";
        String key_value= "8403";
        master_slaver_relation.put("192.123.3.91:"+key_port,"192.123.23.204:"+key_value);
        master_slaver_relation.put("192.123.3.92:"+key_port,"192.123.23.205:"+key_value);
        master_slaver_relation.put("192.123.3.93:"+key_port,"192.123.23.207:"+key_value);
        master_slaver_relation.put("192.123.38.165:"+key_port,"192.123.25.165:"+key_value);
        master_slaver_relation.put("192.123.38.166:"+key_port,"192.123.25.166:"+key_value);
        master_slaver_relation.put("192.123.38.167:"+key_port,"192.123.25.167:"+key_value);
        master_slaver_relation.put("192.123.38.168:"+key_port,"192.123.25.168:"+key_value);
        master_slaver_relation.put("192.123.38.169:"+key_port,"192.123.25.169:"+key_value);
        master_slaver_relation.put("192.123.38.170:"+key_port,"192.123.26.130:"+key_value);
        String hosts ="192.123.3.91:6403 192.123.3.92:6403 192.123.3.93:6403 192.123.38.165:6403 192.123.38.166:6403 192.123.38.167:6403 192.123.38.168:6403 192.123.38.169:6403 192.123.38.170:6403";
        if(args!=null && args.length  >0){
            StringBuffer sb1 = new StringBuffer();
            for (String str : args) {
                System.out.println(str);
                sb1.append(str).append(" ");
            }
             hosts = sb1.toString().substring(0,sb1.toString().length()-1);
        }
        String[] hostAndPorts = hosts.split(" ");
        Set<String> stringSet = new HashSet<String>();
        for (String hostAndPort: hostAndPorts) stringSet.add(hostAndPort);
        StringBuffer sb = new StringBuffer();
        for (String hostAndPort : stringSet) {
//            System.out.println(hostAndPort);
            hostAndPort = master_slaver_relation.get(hostAndPort) ;
            sb.append(hostAndPort).append(" ");
        }
        System.out.println("old:"+hosts);
        if(sb.length() > 0) System.out.println("new:"+sb.toString().substring(0,sb.toString().length()-1));
    }
}
