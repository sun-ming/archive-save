package com.chanct.archivesave.job;

import org.apache.spark.broadcast.Broadcast;

import java.util.Properties;

/**
 * Copyright@https://sun-ming.github.io
 * Author:sunming
 * Date:2018/5/30
 * Description:
 *
 */
public abstract class BaseJob implements IJob {

    protected String date = null;
    protected Broadcast<Properties> propertiesBroadcast = null;
    protected String schemaUrl = null;

    public void init(String date,Broadcast<Properties> propertiesBroadcast,String schemaUrl){
        this.date =date;
        this.propertiesBroadcast = propertiesBroadcast;
        this.schemaUrl = schemaUrl;
    }

}
