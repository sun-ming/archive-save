package com.test.archivesave.output;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;


/**
 * Copyright@https://sun-ming.github.io
 * Author:sunming
 * Date:2018/5/30
 * Description:
 *
 */
public class EsOutPut implements Serializable {

    private static volatile EsOutPut instance = null;
    public static EsOutPut getInstance() {
        if(instance == null) {
            synchronized(EsOutPut.class) {
                if(instance == null) {
                    instance = new EsOutPut();
                }
            }
        }
        return instance;
    }
    private EsOutPut() {
    }


    private String hiveDbname = null;
    private String hiveTablename = null;
    private int hiveHttploaderBatch = 10000;
    private String[] httpLoaderList = null;
    private int httploaderlength = 0;
    private int CurrentIdx = 0;
    private String hiveSchemaUrl = null;

    /**
     * hive.dbname=es
     * hive.tablename=t_seq_reverse_index
     * hive.httploader.batch=10000
     * hive.httploader.list=172.17.20.219:10080,172.17.20.222:10080,172.17.20.223:10080
     * hive.schema.url=http://172.17.20.211:8081
     *
     * @param properties
     */
    public void init(Properties properties){
        hiveDbname = properties.getProperty("hive.dbname");
        hiveTablename = properties.getProperty("hive.tablename");
        hiveHttploaderBatch = Integer.valueOf(properties.getProperty("hive.httploader.batch"));
        httpLoaderList = properties.getProperty("hive.httploader.list").split(",");
        httploaderlength = httpLoaderList.length;
        CurrentIdx = new Random().nextInt(httploaderlength);
        hiveSchemaUrl = properties.getProperty("hive.schema.url");
    }


    public int print(List list) throws Exception {
        String topic = this.getTopic(hiveDbname,hiveTablename);
        return print(list, topic);
    }


    public int print(List list, String topic) throws Exception {
        Schema.Parser parser = new Schema.Parser();
        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(hiveSchemaUrl,100);
        Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());
        System.out.println("EsOutPut print:"+topic+":"+list.size());
        return printByProducer(list, schema, topic, "");
    }

    public int printByOtherHttpLoaders(List list, String topic, String httpurllist) throws Exception {
        Schema.Parser parser = new Schema.Parser();
        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(hiveSchemaUrl,100);
        Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());
        System.out.println("EsOutPut print:"+topic+":"+list.size());
        return printByProducer(list, schema, topic, httpurllist);
    }

    /**
     * get topic of table
     * hive-dbname-tablename
     * @param dbName
     * @param tableName
     * @return
     */
    private String getTopic(String dbName, String tableName) {
        //return "hive-"+dbName+"-"+tableName;
        return tableName;
    }

	public int print(List records, Schema schema) throws Exception {
        String topic = this.getTopic(hiveDbname,hiveTablename);
        return printByProducer(records, schema, topic, "");
	}

	public int printByProducer(List records, Schema schema, String topic, String httpurllist) throws Exception
    {
        ByteArrayOutputStream out = out = new ByteArrayOutputStream(1024*1024*5);
        BinaryEncoder encoder = encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        int keypart = 0;
        int count = 0;//recoreds count
        //System.out.println("Enter EsOutPut.print in thread" + Thread.currentThread().getId());
        String[] localloaderlist = {""};
        int locallistlength = 0;
        int locallistidx = 0;
        String httpbaseurl = "";
        if (!httpurllist.isEmpty())
        {
            localloaderlist = httpurllist.split(",");
            locallistlength = localloaderlist.length;
            locallistidx = new Random().nextInt(locallistlength);
        }
        for (int i = 0;i<records.size();i++) {
            keypart++;
            count++;
            GenericRecord payload = new GenericData.Record(schema);
            Map<String, Object> map = (Map<String, Object>) records.get(i);
            List<Field> fields = schema.getFields();
            for (int j = 0;j<fields.size();j++) {
                Field field_item = fields.get(j);
                String field_name = field_item.name();
                Schema field_schema = field_item.schema();
                payload.put(j, BuildAvroObj(map.get(field_name), field_schema));
            }
            writer.write(payload, encoder);
            if (count >= hiveHttploaderBatch) {
                encoder.flush();
                out.flush();
                byte[] payloadBytes = out.toByteArray();
                //System.out.println("EsOutPut prepare sendoutbytes in for loop in thread" + Thread.currentThread().getId());
                if (locallistlength > 0)
                {
                    httpbaseurl = localloaderlist[locallistidx];
                    locallistidx ++;
                    if (locallistidx >= locallistlength)
                    {
                        locallistidx = 0;
                    }
                }
                SendOutBytes(topic, keypart, payloadBytes, httpbaseurl);
                out.reset();
                count = 0;
            }
        }
        if(encoder!=null){
            encoder.flush();
        }
        if (count > 0) {
            out.flush();
            byte[] serializedBytes = out.toByteArray();
            //System.out.println("EsOutPut prepare sendoutbytes out of loop in thread" + Thread.currentThread().getId());
            if (locallistlength > 0)
            {
                httpbaseurl = localloaderlist[locallistidx];
                locallistidx ++;
                if (locallistidx >= locallistlength)
                {
                    locallistidx = 0;
                }
            }
            SendOutBytes(topic, keypart, serializedBytes, httpbaseurl);
        }
        if(out!=null){
            out.flush();
        }
        return count;
    }

    private Object BuildAvroObj(Object o, Schema field_schema) {
        Schema.Type field_type = field_schema.getType();
        if (field_type == Schema.Type.ARRAY)
        {
            Schema elementType = field_schema.getElementType();
            GenericArray arrayDatum = new GenericData.Array(0, field_schema);
            for (Object elementObj : (Collection) o) {
                arrayDatum.add(BuildAvroObj(elementObj, elementType));
            }
            return arrayDatum;
        } else if (field_type == Schema.Type.RECORD){
            GenericRecord recordDatum = new GenericData.Record(field_schema);
            List<Field> fields = field_schema.getFields();
            HashMap<String, Object> SubMap = (HashMap<String, Object>) o;
            for (int j = 0;j<fields.size();j++) {
                Field field_item = fields.get(j);
                String field_name = field_item.name();
                Schema record_field_schema = field_item.schema();
                recordDatum.put(j, BuildAvroObj(SubMap.get(field_name), record_field_schema));
            }
            return recordDatum;
        }
        return o;
    }

    private void SendOutBytes(String topic, int keypart, byte[] payloadBytes, String httpbaseurl) throws IOException {
	    if (payloadBytes.length == 0)
        {
            System.out.println("Warning: EsOutPut get empty bytes in thread" + Thread.currentThread().getId());
        }
	    if (httploaderlength == 0) {
            System.out.println("Warning: EsOutPut has no http loader, data will be discard. Thread ID: " + Thread.currentThread().getId());
            return;
        }

        if (httpbaseurl.isEmpty()) {
            httpbaseurl = GetNextBaseUrl();
        }
        System.out.println("EsOutPut SendOutBytes using http "+httpbaseurl);
        String loadurl = "http://" + httpbaseurl;
        DefaultHttpClient httpClient = new DefaultHttpClient();
        HttpPost request = new HttpPost(loadurl);
        //System.out.println("Http uploading to:" + loadurl);
        request.addHeader("content-type", "binary/octet-stream");
        //request.addHeader("User", "LiMing");
        //request.addHeader("Password", "123");
        request.addHeader("Topic", topic);
        request.addHeader("Format", "avro");
        HttpEntity httpEntity = new ByteArrayEntity(payloadBytes);
        request.setEntity(httpEntity);
        HttpResponse response = httpClient.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (200 != statusCode) {
            System.out.println("HiveHttpOutPut http uploading(" + httpbaseurl + ") response code "+  statusCode + " for topic: " +
                    topic + ", response:" + response);
        }
    }

    private synchronized String GetNextBaseUrl() {
        String result = httpLoaderList[CurrentIdx];
        CurrentIdx ++;
        if (CurrentIdx >= httploaderlength)
        {
            CurrentIdx = 0;
        }
        return result;
    }
}
