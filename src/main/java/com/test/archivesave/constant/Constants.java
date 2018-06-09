package com.test.archivesave.constant;

import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Copyright@https://sun-ming.github.io
 * Author:sunming
 * Date:2018/5/30
 * Description:
 *
 */
public interface Constants extends Serializable {


    List<Tuple3<String,String,String>> ATTR_KEY_VALUE_TABLE_LIST =
            new ArrayList<Tuple3<String,String,String>>(){
                {
                    add(new Tuple3<String, String, String>("imei","imei","s_hardware"));
                    add(new Tuple3<String, String, String>("imsi","imsi","s_hardware"));
                    add(new Tuple3<String, String, String>("idNum","idcard","s_profile"));
                    add(new Tuple3<String, String, String>("bankCard","bankcard","s_profile"));
                    add(new Tuple3<String, String, String>("plateNum","platenum","s_profile"));
                    add(new Tuple3<String, String, String>("Name","name","s_profile"));
                    add(new Tuple3<String, String, String>("weixinNick","wx","s_vid"));
                    add(new Tuple3<String, String, String>("qq","qq","s_vid"));
                    add(new Tuple3<String, String, String>("taobaoNick","taobao","s_vid"));
                    add(new Tuple3<String, String, String>("weiboNick","weibo","s_vid"));
                    add(new Tuple3<String, String, String>("email","email","s_vid"));
                }
            };


    List<String> TOPIC_ARRAY = new ArrayList<String>(){
        {
            add("s_passwd");
            add("s_vid");
            add("s_profile");
            add("s_hardware");
        }
    };


}
