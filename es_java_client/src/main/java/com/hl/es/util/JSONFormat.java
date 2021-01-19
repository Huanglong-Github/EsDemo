package com.hl.es.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.serializer.SimpleDateFormatSerializer;

import java.sql.Timestamp;
import java.util.Date;

/**
 * @author: huanglong60
 * @date: 2021/1/15 13:23
 * @description:
 */

public class JSONFormat {
    private static ObjectSerializer BIG_LONG_SERIALIZER = new BigLongSerializer();

    public JSONFormat() {
    }

    public static String toJSONString(Object object, String dateFormat) {
        SimpleDateFormatSerializer simpleDateFormatSerializer = new SimpleDateFormatSerializer(dateFormat);
        SerializeConfig mapping = new SerializeConfig();
        mapping.put(Date.class, simpleDateFormatSerializer);
        mapping.put(Timestamp.class, simpleDateFormatSerializer);
        mapping.put(Long.class, BIG_LONG_SERIALIZER);
        return JSON.toJSONString(object, mapping, new SerializerFeature[0]);
    }
}