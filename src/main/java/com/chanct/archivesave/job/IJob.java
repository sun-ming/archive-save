package com.chanct.archivesave.job;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Copyright@https://sun-ming.github.io
 * Author:sunming
 * Date:2018/5/30
 * Description:
 *
 */
public interface IJob extends Serializable {

    void doJob(SparkSession sparkSession, JavaSparkContext javaSparkContext);

}
