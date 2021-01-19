package com.hl.es.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author: huanglong60
 * @date: 2021/1/17 15:14
 * @description: 自定义注解，用于反射得到index的id
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface EsDocumentId {
}