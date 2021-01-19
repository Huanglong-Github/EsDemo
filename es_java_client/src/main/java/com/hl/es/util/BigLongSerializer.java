package com.hl.es.util;

import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import org.elasticsearch.client.Client;
import org.springframework.beans.factory.annotation.Autowired;

import javax.swing.event.CaretListener;
import java.io.IOException;
import java.lang.reflect.Type;

/**
 * @author: huanglong60
 * @date: 2021/1/15 13:24
 * @description:
 */
public class BigLongSerializer implements ObjectSerializer {
    public BigLongSerializer() {
    }

    @Autowired
    Client client;

    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        if (object == null) {
            serializer.getWriter().writeNull();
        } else {
            if ((Long)object < 100000000000000L) {
                serializer.getWriter().writeLong((Long)object);
            } else {
                serializer.write(String.valueOf(object));
            }

        }
    }
}
