package com.test.archivesave.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Copyright@https://sun-ming.github.io
 * Author:sunming
 * Date:2018/5/30
 * Description:
 *
 */
public class HbaseOutPut implements Serializable {
    private static volatile HbaseOutPut instance = null;
    public static HbaseOutPut getInstance() {
        if(instance == null) {
            synchronized(HbaseOutPut.class) {
                if(instance == null) {
                    instance = new HbaseOutPut();
                }
            }
        }
        return instance;
    }
    private HbaseOutPut() {
    }

    private String zookeeperList = null;
    private int hbaseLoaderBatch = 10000;
    //private String hbaseNamespace = null;
    private String hbaseTable = null;

    public void init(Properties properties) {
        String zkQuorum = properties.getProperty("zkQuorum");
        zookeeperList = parseQuorum(zkQuorum);
        hbaseLoaderBatch = Integer.valueOf(properties.getProperty("hbase.loader.batch"));
        //hbaseNamespace = properties.getProperty("hbase.namespace");
        hbaseTable = properties.getProperty("hbase.table");
    }

    private String parseQuorum(String zkQuorum) {
        StringBuilder sb = new StringBuilder();
        boolean IsFirst = true;
        for (String ServerIPort : zkQuorum.split("/")[0].split(",")) {
            if (IsFirst == true) {
                IsFirst = false;
            } else {
                sb.append(",");
            }
            sb.append(ServerIPort.split(":")[0]);
        }
        return sb.toString();
    }

    public int print(List list) throws Exception {
        //String topic = this.getTopic(hbaseNamespace, hbaseTable);
        String topic = this.getTopic(hbaseTable);
        return print(list, topic);
    }

    private String getTopic(String namespace, String table) {
        return namespace + ":" + table;
    }
    private String getTopic(String table) {
        return table;
    }

    public int print(List list, String topic) throws Exception {
        System.out.println("HbaseOutPut print:"+topic+":"+list.size());

        Configuration conf = getHBaseConf();
        HTable table1 = new HTable(conf, topic);
        table1.setAutoFlush(false);
        table1.setWriteBufferSize(5*1024*1024);
        List<Put> putData = new ArrayList<Put>();
        Long tNow = System.currentTimeMillis() / 1000;

        boolean isFirst = true;
        String sampleRowKey = "";
        String columnFamily = "";
        int count = 0;
        for (Object obj : list) {
            Map<String, Object> hbaseMap = (Map<String, Object>) obj;
            if (!hbaseMap.containsKey("RowKey")) {
                continue;
            }
            String rowKey = (String) hbaseMap.remove("RowKey");
            if (hbaseMap.containsKey("ColumnFamily")) {
                columnFamily = (String) hbaseMap.remove("ColumnFamily");
            }
            Long ts = 0l;
            if (hbaseMap.containsKey("TimeStamp")) {
                ts = (Long) hbaseMap.remove("TimeStamp");
                //ts *= 1000; //Convert to milliseconds
            }
            if (isFirst == true) {
                isFirst = false;
                System.out.println("HbaseOutPut sample rowKey:"+rowKey);
                sampleRowKey = rowKey;
            }
            Put put = new Put(Bytes.toBytes(rowKey));
            for(Map.Entry<String, Object> data: hbaseMap.entrySet()){
                if(data.getValue() != null){
                    if (ts == 0l) {
                        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(data.getKey().toString()),
                                Bytes.toBytes(data.getValue().toString()));
                    } else {
                        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(data.getKey().toString()),
                                ts, Bytes.toBytes(data.getValue().toString()));
                    }
                    putData.add(put);
                }
            }
            count ++;

            if (count >= hbaseLoaderBatch) {
                table1.put(putData);
                table1.flushCommits();
                putData.clear();
                count = 0;
            }
        }
        if (count > 0) {
            table1.put(putData);
            table1.flushCommits();
            putData.clear();
        }
        table1.close();
        System.out.println("HbaseOutPut finished print:"+topic+": "+list.size() + ", sample row key:" + sampleRowKey);
        return 0;
    }

    private Configuration getHBaseConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", zookeeperList);
        conf.set("hbase.client.keyvalue.maxsize", "20971520");
        return conf;
    }


}
