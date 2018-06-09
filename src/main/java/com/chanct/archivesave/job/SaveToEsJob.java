package com.chanct.archivesave.job;

import com.chanct.archivesave.constant.Constants;
import com.chanct.archivesave.output.EsOutPut;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.*;

/**
 * Copyright@https://sun-ming.github.io
 * Author:sunming
 * Date:2018/5/30
 * Description:
 *
 */
public class SaveToEsJob extends BaseJob {

    public void doJob(SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        String sql = makeSql();
        System.out.println("saveToEs sql is : "+sql);
        //Dataset<Row> ds = sparkSession.sql(sql);
        sparkSession.sql(sql).javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> rowIterator) throws Exception {

                List list = new ArrayList();
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    String phoneNum = row.getString(0).trim();
                    //如果手机号码不是数字，不保存
                    if(!StringUtils.isNumeric(phoneNum.replace("_",""))){
                        continue;
                    }
                    String attriName = row.getString(1).trim();
                    String attriValue = row.getString(2).trim();
                    long date = row.get(3)==null?0L:row.getLong(3);
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
        });
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

    private String makeSql() {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i< Constants.ATTR_KEY_VALUE_TABLE_LIST.size(); i++) {
            Tuple3<String,String,String> tuple3 = Constants.ATTR_KEY_VALUE_TABLE_LIST.get(i);
            String sample = "SELECT DISTINCT phonenum,'%s' AS k,%s as v,time FROM %s " +
                    "WHERE day='%s' AND phonenum is not null AND %s is not null ";
            String sqlPart = String.format(sample,tuple3._1(),tuple3._2(),tuple3._3(),date,tuple3._2());
            sb.append(sqlPart);
            if (i<(Constants.ATTR_KEY_VALUE_TABLE_LIST.size()-1)) {
                sb.append("UNION ALL ");
            }
        }
        return sb.toString();
    }

}
