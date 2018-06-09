package com.test.archivesave.job;

import com.test.archivesave.constant.Constants;
import com.test.archivesave.output.HbaseOutPut;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.avro.Schema.Type.RECORD;

/**
 * Copyright@https://sun-ming.github.io
 * Author:sunming
 * Date:2018/5/30
 * Description:
 *
 */
public class SaveToHbaseJob extends BaseJob {

    public void doJob(SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        for (String topic: Constants.TOPIC_ARRAY) {
            Schema schema = getSchema(topic);
            final Broadcast<Schema> schemaBroadcast = javaSparkContext.broadcast(schema);
            final Broadcast<String> topicBroadcast = javaSparkContext.broadcast(topic);
            String sql = makeSqlFromSchema(topic,schema);
            System.out.println("hive table : "+topic+" , sql is : "+sql);
            sparkSession.sql(sql).javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
                @Override
                public void call(Iterator<Row> rowIterator) throws Exception {
                    List list = new ArrayList();
                    List<Schema.Field> fields = schemaBroadcast.getValue().getFields();
                    String topic = topicBroadcast.value();
                    while (rowIterator.hasNext()) {
                        Map map = new HashMap();
                        map.put("ColumnFamily",topic);
                        Row row = rowIterator.next();
                        for (int i=0;i<fields.size();i++) {
                            if(i==0) {
                                String phoneNum = row.getString(i).trim();
                                //如果手机号码不是数字，不保存
                                if(!StringUtils.isNumeric(phoneNum.replace("_",""))){
                                    continue;
                                }
                                map.put("RowKey",phoneNum);
                                map.put(fields.get(i).name(),phoneNum);
                            } else {
                                Object result = row.get(i);
                                if (result!=null){
                                    Object value = convert(fields.get(i).schema(), result.toString().trim());
                                    map.put(fields.get(i).name(),value);
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
        }
    }

    private Object convert(Schema schema, String v) throws Exception {
        Object o = null;
        if (v == null) {
            return null;
        }
        if (v.length() == 0 || v.equals("null")) {
            switch (schema.getType()) {
                case STRING:
                    return v;
                case BYTES:
                    return ByteBuffer.wrap(v.getBytes("UTF-8"));
                case UNION:
                    break;
                default:
                    return null;
            }
        }

        switch (schema.getType()) {
            case NULL:
                o = null;
                break;
            case BOOLEAN:
                o = Boolean.parseBoolean(v);
                break;
            case INT:
                o = Integer.parseInt(v);
                break;
            case LONG:
                o = Long.parseLong(v);
                break;
            case FLOAT:
                o = Float.parseFloat(v);
                break;
            case DOUBLE:
                o = Double.parseDouble(v);
                break;
            case BYTES:
                try {
                    o = ByteBuffer.wrap(v.getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                break;
            case STRING:
                o = v;
                break;
            case UNION:
                for (Schema mem : schema.getTypes()) {
                    o = convert(mem, v);
                    // break wehn encounter not null value, or we will get null
                    if (o != null) break;
                }
                break;
            case RECORD:
                throw new Exception("Unsopported avro type:" + RECORD);
            case MAP:

                throw new Exception("Unsopported avro type:" + RECORD);
            case ENUM:
                throw new Exception("Unsopported avro type:" + RECORD);
            case ARRAY:
                throw new Exception("Unsopported avro type:" + RECORD);
            case FIXED:
                throw new Exception("Unsopported avro type:" + RECORD);
            default:
                throw new Exception("Unsopported avro type:" + RECORD);
        }
        return o;
    }

    private Schema getSchema(String topic) {
        Schema.Parser parser = new Schema.Parser();
        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(schemaUrl, 100);
        Schema schema = null;
        try {
            schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());
            if (null == schema) {
                System.out.println("在指定url" + schemaUrl + "下未获取到" + topic);
                return null;
            }
            int size = schema.getFields().size();
            System.out.println(topic + "共有" + size + "个字段");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
        return schema;
    }

    private String makeSqlFromSchema(String topic,Schema schema) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT DISTINCT ");
        for (Schema.Field field: schema.getFields()){
            sb.append(field.name().trim().toLowerCase()).append(",");
        }
        if (sb.charAt(sb.length()-1)==',') {
            sb.deleteCharAt(sb.length()-1);
        }
        sb.append(" FROM "+topic+" WHERE day='"+date+"' AND phonenum IS NOT NULL");
        return sb.toString();
    }

}
