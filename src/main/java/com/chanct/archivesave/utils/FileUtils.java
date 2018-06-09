package com.chanct.archivesave.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URL;

/**
 * Copyright@https://sun-ming.github.io
 * Author:sunming
 * Date:2018/5/30
 * Description:
 *
 */
public class FileUtils implements Serializable {

    public static InputStream OpenURLOrLocalFile(String path) throws IOException {
        try {
            if (path.startsWith("http://")){
                return new URL(path).openStream();
            }else if (path.startsWith("hdfs://")) {
                return FileSystem.get(URI.create(path.replace("hdfs://","")), new Configuration()).open(new Path(path.replace("hdfs://","")));
            }else {
                if(exist(path)){
                    return new FileInputStream(new File(path));
                }else{
                    throw new FileNotFoundException(path);
                }
            }
        }catch (Exception e){
            throw new FileNotFoundException(path);
        }
    }

    private static boolean exist(String path) {
        if ((path == null) || (path.trim().length() == 0)) {
            return false;
        }
        return new File(path).exists();
    }


}
