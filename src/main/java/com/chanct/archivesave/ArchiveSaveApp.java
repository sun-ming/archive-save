package com.chanct.archivesave;

import com.chanct.archivesave.output.HbaseOutPut;
import com.chanct.archivesave.output.EsOutPut;
import com.chanct.archivesave.utils.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Copyright@https://sun-ming.github.io
 * Author:sunming
 * Date:2018/5/30
 * Description:
 *
 */
public class ArchiveSaveApp implements Serializable {
    private static String date = null;
    private static String confPath = null;
    private static Properties properties = null;
    private static String schemaUrl = null;
    private static Broadcast<Properties> propertiesBroadcast = null;

    public static void main(String[] args) {
        //初始化相关参数，并做相关校验
        // "20180528" "http://x.x.x.x:80/config/seq/seq_archive_save.properties"
        if (args.length!=2) {
            System.out.println("this program need two args:date and confPath");
            return;
        } else {
            init(args);
        }

        if (properties.isEmpty()){
            System.out.println("there is not config properties");
            return;
        }

        boolean isSaveToES= Boolean.valueOf(properties.getProperty("isSaveToES"));
        boolean isSaveToHbase = Boolean.valueOf(properties.getProperty("isSaveToHbase"));

        //初始化spark相关，把配置参数广播
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.shuffle.consolidateFiles","true");
        sparkConf.registerKryoClasses(new Class[]{Properties.class,EsOutPut.class, HbaseOutPut.class});
        SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        propertiesBroadcast = javaSparkContext.broadcast(properties);
        String sql = " USE seq_analysis ";
        System.out.println("hive sql : "+sql);
        sparkSession.sql(sql);

        //结果保存到ES
        if (isSaveToES) {
            System.out.println("save to ES begin...");
            String clazzStr = "com.chanct.archivesave.job.SaveToEsJob";
            execute(sparkSession, javaSparkContext, clazzStr);
            System.out.println("save to ES over!!!");
        } else {
            System.out.println("conf file means don't save to ES,please check conf file");
        }

        //结果保存到Hbase
        if (isSaveToHbase) {
            System.out.println("save to HBase begin...");
            String clazzStr = "com.chanct.archivesave.job.SaveToHbaseJob";
            execute(sparkSession, javaSparkContext, clazzStr);
            System.out.println("save to HBase over!!!");
        } else {
            System.out.println("conf file means don't save to HBase,please check conf file");
        }

    }

    private static void execute(SparkSession sparkSession, JavaSparkContext javaSparkContext, String clazzStr) {
        try{
            Class clazz = Class.forName(clazzStr);
            Object instance = clazz.newInstance();
            Method initMethod = clazz.getMethod("init",String.class,Broadcast.class,String.class);
            initMethod.invoke(instance,date,propertiesBroadcast,schemaUrl);
            Method doJobMethod = clazz.getMethod("doJob",SparkSession.class,JavaSparkContext.class);
            doJobMethod.invoke(instance,sparkSession,javaSparkContext);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void init(String[] args) {
        date = args[0];
        confPath = args[1];
        properties = new Properties();
        InputStream in = null;
        try {
            in = FileUtils.OpenURLOrLocalFile(confPath);
            properties.load(in);
            schemaUrl = properties.getProperty("hive.schema.url");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }

}
