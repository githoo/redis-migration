package com.githoo.tool.redismigration.conf;

import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;


public class ConfigParseUtil {
    /**
     * 提供Properties属性的默认加载，同时可以通过 p 更改其加载路径
     * 同时可以根据 e 替换 已加载 属性值
     * @param args
     * @param defaultConf
     * @throws java.io.IOException
     * @throws ParseException
     */
    public static Properties parse(String[] args,String defaultConf) throws IOException, ParseException {
        return parse(args,defaultConf,"p","e");
    }
    /**
     * 提供Properties属性的默认加载，同时可以通过 newConfCmdPara 更改其加载路径
     * 同时可以根据 replaceCmdPara 替换 已加载 属性值
     * @param args
     * @param defaultConf
     * @param newConfCmdPara
     * @param replaceCmdPara
     * @throws java.io.IOException
     * @throws ParseException
     */
    public static Properties parse(String[] args,String defaultConf,String newConfCmdPara,String replaceCmdPara) throws IOException, ParseException {
        CommandLineParser parser = new PosixParser();
        Options options = buildOptions(newConfCmdPara,replaceCmdPara);
        CommandLine cmd = parser.parse(options, args);
        Properties properties = new Properties();
        InputStream inputStream;
        if (cmd.hasOption(newConfCmdPara)) {
            File file = new File(cmd.getOptionValue(newConfCmdPara));
            inputStream = new FileInputStream(file);
        } else {
            inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(defaultConf);
        }
        properties.load(inputStream);
        if (cmd.hasOption(replaceCmdPara)) {
            try {
                Properties extendProperties = cmd.getOptionProperties(replaceCmdPara);
                Enumeration en = extendProperties.propertyNames();
                printFrame(100,"","!");
                while(en.hasMoreElements()){
                    String key = en.nextElement().toString();//key值
                    String oldValue=properties.get(key).toString();
                    properties.put(key, extendProperties.get(key));
                    String newValue=properties.get(key).toString();
                    printFrame(100, String.format("properties old  %s : %s",key,oldValue),"!");
                    printFrame(100, String.format("properties new  %s : %s",key,newValue),"!");
                }
                printFrame(100,"","!");
            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
            }
        }
        return properties;
    }

    /**
     * 构建命令行扩展解析参数
     * @return
     */
    public static Options buildOptions() {
        return buildOptions("p","e");
    }

    /**
     * 构建两个命令行参数
     * @param newConfCmdPara “例如 -p /export/a.pro”
     * @param replaceCmdPara "例如 -e zk.server=1.1.1.1 "
     * @return
     */
    public static Options buildOptions(String newConfCmdPara,String replaceCmdPara) {
        Options options = new Options();
        options.addOption(String.valueOf(newConfCmdPara), true, "external properties filename");
        options.addOption(OptionBuilder
                .withArgName("property=value")
                .hasArgs(2)
                .withValueSeparator()
                .withDescription("extend info for defalut load properties ").create(replaceCmdPara));

        return options;
    }

    /**
     * 查找制定配置文件具体配置，不存在返回 调用者制定默认值
     *
     * @param properties
     * @param name
     * @param defaultValue
     * @return
     * @throws Exception
     */
    public static String getProperty(Properties properties,String name, String defaultValue) throws Exception {
        String value = properties.getProperty(name);
        if (value == null || value.isEmpty()) {
            return defaultValue.trim();
        } else {
            return value.trim();
        }
    }

    /**
     * 查找制定配置文件具体配置，不存在则抛出异常
     *
     * @param properties
     * @param name
     * @return
     * @throws Exception
     */
    public static String getProperty(Properties properties,String name) throws Exception {
        String value = properties.getProperty(name);
        if (value == null || value.isEmpty()) {
            throw new Exception("no parameters(" + name + ") set!");
        } else {
            return value.trim();
        }
    }

    /**
     *   打印property配置信息内容 默认是* 如下
     *      *******************************************
     *      *properties    storm.num.workers : 10     *
     *      *properties    storm.num.spouts : 20      *
     *      *******************************************
     * @param properties
     * @param width
     */
    public static void printProperty(Properties properties,int width){
        printProperty(properties,width,"*");
    }

    /**
     * 打印property配置信息内容
     * @param properties
     * @param width
     * @param printChar
     */
    public static void printProperty(Properties properties,int width,String printChar){
        Enumeration en = properties.propertyNames();
        printFrame(width, "",printChar);
        while(en.hasMoreElements()){
            String key = en.nextElement().toString();//key值
            String value=properties.get(key).toString();
            if (value.length() + 21 > width) {
                width = value.length() + 21;
            }
            printFrame(width, String.format("properties    %s : %s",key,value),printChar);
        }
        printFrame(width, "",printChar);
    }
    /**
     * 格式化输出信息例如 默认是*填充
     *
     * @param width  如果是空的话输出多少 printChar
     * @param message  消息
     */
    private static void printFrame(int width, String message) {
        printFrame(width,message,"*");
    }

    /**
     * 格式化输出信息例如
     *
     * @param width  如果是空的话输出多少 printChar
     * @param message  消息
     * @param printChar 格式化填充的字符
     */
    private static void printFrame(int width, String message,String printChar) {
        if (message == null || message.isEmpty()) {
            for (int i = 0; i < width; ++i) {
                System.out.print(printChar);
            }
        } else {
            for (int i = 0; i < width; ++i) {
                if (i == 0 || i == width - 1) {
                    System.out.print(printChar);
                } else if (i == 2) {
                    System.out.print(message);
                    i = i + message.length() - 1;
                } else {
                    System.out.print(" ");
                }
            }
        }
        System.out.print("\n");
    }



}
