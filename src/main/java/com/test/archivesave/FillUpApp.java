package com.test.archivesave;

import com.test.archivesave.output.EsOutPut;
import com.test.archivesave.output.HbaseOutPut;
import com.test.archivesave.utils.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.*;

/**
 * Copyright@https://sun-ming.github.io
 * Author:sunming
 * Date:2018/5/30
 * Description:
 * 补全数据，把离线库（seq）中的分析结果导入hbase和es
 */
public class FillUpApp implements Serializable {
    private static String date = null;
    private static String confPath = null;
    private static Properties properties = null;
    private static String schemaUrl = null;
    //private static Broadcast<Properties> propertiesBroadcast = null;

    public static void main(String[] args) {
        //初始化相关参数，并做相关校验
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
        final Broadcast<Properties> propertiesBroadcast = javaSparkContext.broadcast(properties);
        String sql = " USE seq ";
        System.out.println("hive sql : "+sql);
        sparkSession.sql(sql);

        //结果保存到ES
        if (isSaveToES) {
            System.out.println("save to ES begin...");
            /*String clazzStr = "com.chanct.archivesave.job.SaveToEsJob";
            execute(sparkSession, javaSparkContext, clazzStr);*/
            String sql1 =
                    "SELECT DISTINCT phonenum,'imei' AS k,imei as v,updatetime FROM s_profile WHERE phonenum is not null AND imei is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'imsi' AS k,imsi as v,updatetime FROM s_profile WHERE phonenum is not null AND imsi is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'idNum' AS k,id as v,updatetime FROM s_profile WHERE phonenum is not null AND id is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'bankCard' AS k,bankcard as v,updatetime FROM s_profile WHERE phonenum is not null AND bankcard is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'plateNum' AS k,carnum as v,updatetime FROM s_profile WHERE phonenum is not null AND carnum is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'Name' AS k,name as v,updatetime FROM s_profile WHERE phonenum is not null AND name is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'weixinNick' AS k,wx as v,updatetime FROM s_usernames WHERE phonenum is not null AND wx is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'qq' AS k,qq as v,updatetime FROM s_usernames WHERE phonenum is not null AND qq is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'taobaoNick' AS k,taobao as v,updatetime FROM s_usernames WHERE phonenum is not null AND taobao is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'weiboNick' AS k,weibo as v,updatetime FROM s_usernames WHERE phonenum is not null AND weibo is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'email' AS k,email as v,updatetime FROM s_usernames WHERE phonenum is not null AND email is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'weixinNick' AS k,wx as v,updatetime FROM s_usernames2 WHERE phonenum is not null AND wx is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'qq' AS k,qq as v,updatetime FROM s_usernames2 WHERE phonenum is not null AND qq is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'taobaoNick' AS k,taobao as v,updatetime FROM s_usernames2 WHERE phonenum is not null AND taobao is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'weiboNick' AS k,weibo as v,updatetime FROM s_usernames2 WHERE phonenum is not null AND weibo is not null "+
                    "UNION ALL "+
                    "SELECT DISTINCT phonenum,'email' AS k,email as v,updatetime FROM s_usernames2 WHERE phonenum is not null AND email is not null ";
            System.out.println("saveToEs sql is : "+sql1);
            sparkSession.sql(sql1).javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
                @Override
                public void call(Iterator<Row> rowIterator) throws Exception {

                    List list = new ArrayList();
                    while (rowIterator.hasNext()) {
                        Row row = rowIterator.next();
                        String phoneNum = row.getString(0).trim();
                        //如果手机号码不是数字，不保存
                        if(!StringUtils.isNumeric(phoneNum.replace("-",""))){
                            continue;
                        }
                        if(phoneNum.length()==18) {
                            phoneNum = phoneNum.replace("-","_");
                        }
                        if(phoneNum.length()==13 && phoneNum.startsWith("861")) {
                            String last4 = phoneNum.substring(phoneNum.length()-4,phoneNum.length());
                            phoneNum = last4+"_"+phoneNum;
                        }
                        if (phoneNum.length()!=18) {
                            continue;
                        }
                        String attriName = row.getString(1).trim();
                        String attriValue = row.getString(2).trim();
                        long date = row.get(3)==null?0L:row.getTimestamp(3).getTime()/1000L;
                        Map map = new HashMap();
                        map.put("phonenum", phoneNum);
                        map.put("attriname", attriName);
                        map.put("attrivalue", attriValue);
                        String idHash = computeIDHash(phoneNum, attriName, attriName);
                        map.put("id", idHash);
                        map.put("date", date);
                        list.add(map);
                    }
                    if (list.size()>0) {
                        Properties prop = propertiesBroadcast.getValue();
                        EsOutPut esOutPut = EsOutPut.getInstance();
                        esOutPut.init(prop);
                        esOutPut.print(list);
                    }
                }
                private String computeIDHash(String msisdn, String property, String value) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("{\"phonenum\":\"").append(msisdn).append("\",")
                            .append("\"attriname\":\"").append(property).append("\",")
                            .append("\"attrivalue\":\"").append(value).append("\"}");
                    String datastr = sb.toString();
                    try {
                        MessageDigest md5 = MessageDigest.getInstance("MD5");
                        md5.update(datastr.getBytes("UTF8"));
                        byte[] resultbytes = md5.digest();
                        BigInteger bi = new BigInteger(1, resultbytes);
                        return bi.toString(16);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "";
                }
            });
            System.out.println("save to ES over!!!");
        } else {
            System.out.println("conf file means don't save to ES,please check conf file");
        }

        //结果保存到Hbase
        if (isSaveToHbase) {
            System.out.println("save to HBase begin...");

            String sql1 =
                    "select distinct " +
                    "phonenum,name,gender,age,birth,company,address,bankcard,salary,capital," +
                    "carnum,vin,id,telephone,updatetime " +
                    "from s_profile where phonenum is not null";
            sparkSession.sql(sql1).javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
                @Override
                public void call(Iterator<Row> rowIterator) throws Exception {
                    List<String> fields = new ArrayList<String>(
                            Arrays.asList(new String[]{
                            "name","gender","age","birth","company",
                            "address","bankcard","salary","asset","platenum",
                            "vin","idcard","telephone","time"}));
                    List list = new ArrayList();
                    while (rowIterator.hasNext()) {
                        Map map = new HashMap();
                        map.put("ColumnFamily","s_profile");
                        Row row = rowIterator.next();
                        String phoneNum = row.getString(0).trim();
                        if(!StringUtils.isNumeric(phoneNum.replace("-",""))){
                            continue;
                        }
                        if(phoneNum.length()==18) {
                            phoneNum = phoneNum.replace("-","_");
                        }
                        if(phoneNum.length()==13 && phoneNum.startsWith("861")) {
                            String last4 = phoneNum.substring(phoneNum.length()-4,phoneNum.length());
                            phoneNum = last4+"_"+phoneNum;
                        }
                        if (phoneNum.length()!=18) {
                            continue;
                        }
                        map.put("RowKey",phoneNum);
                        map.put("phonenum",phoneNum);
                        for (int i=0;i<fields.size();i++) {
                            if (row.get(i+1)!=null){
                                if (i!=(fields.size()-1)) {
                                    String value = row.getString(i+1);
                                    map.put(fields.get(i),value);
                                } else {
                                    Timestamp value = row.getTimestamp(i+1);
                                    map.put(fields.get(i),value.getTime()/1000L);
                                }

                            }

                        }
                        list.add(map);
                    }

                    if (list.size()>0) {
                        Properties prop = propertiesBroadcast.getValue();
                        HbaseOutPut hbaseOutPut = HbaseOutPut.getInstance();
                        hbaseOutPut.init(prop);
                        hbaseOutPut.print(list);
                    }
                }
            });

            String sql2 =
                    "select distinct "+
                    "phonenum,qq,wx,weibo,email,jd,alipay,taobao, "+
                    "webstite as website,webaccount,appname,appcount as appacount,updatetime "+
                    "from s_usernames where phonenum is not null "+
                    "union all "+
                    "select distinct "+
                    "phonenum,qq,wx,weibo,email,jd,alipay,taobao, "+
                    "website,webaccount,appname,appacount,updatetime "+
                    "from s_usernames2 where phonenum is not null ";
            sparkSession.sql(sql2).javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
                @Override
                public void call(Iterator<Row> rowIterator) throws Exception {
                    List<String> fields = new ArrayList<String>(
                            Arrays.asList(new String[]{
                                    "qq","wx","weibo","email","jd",
                                    "alipay","taobao","website","webaccount","appname",
                                    "appaccount","time"}));
                    List list = new ArrayList();
                    while (rowIterator.hasNext()) {
                        Map map = new HashMap();
                        map.put("ColumnFamily","s_vid");
                        Row row = rowIterator.next();
                        String phoneNum = row.getString(0).trim();
                        if(!StringUtils.isNumeric(phoneNum.replace("-",""))){
                            continue;
                        }
                        if(phoneNum.length()==18) {
                            phoneNum = phoneNum.replace("-","_");
                        }
                        if(phoneNum.length()==13 && phoneNum.startsWith("861")) {
                            String last4 = phoneNum.substring(phoneNum.length()-4,phoneNum.length());
                            phoneNum = last4+"_"+phoneNum;
                        }
                        if (phoneNum.length()!=18) {
                            continue;
                        }
                        map.put("RowKey",phoneNum);
                        map.put("phonenum",phoneNum);
                        for (int i=0;i<fields.size();i++) {
                            if (row.get(i+1)!=null){
                                if (i!=(fields.size()-1)) {
                                    String value = row.getString(i+1);
                                    map.put(fields.get(i),value);
                                } else {
                                    Timestamp value = row.getTimestamp(i+1);
                                    map.put(fields.get(i),value.getTime()/1000L);
                                }

                            }

                        }
                        list.add(map);
                    }

                    if (list.size()>0) {
                        Properties prop = propertiesBroadcast.getValue();
                        HbaseOutPut hbaseOutPut = HbaseOutPut.getInstance();
                        hbaseOutPut.init(prop);
                        hbaseOutPut.print(list);
                    }
                }
            });

            System.out.println("save to HBase over!!!");
        } else {
            System.out.println("conf file means don't save to HBase,please check conf file");
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
