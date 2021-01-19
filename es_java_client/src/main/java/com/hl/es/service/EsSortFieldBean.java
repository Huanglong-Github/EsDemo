package com.hl.es.service;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.sort.SortOrder;

/**
 * @author: huanglong60
 * @date: 2021/1/17 23:25
 * @description:
 */
@Data
@NoArgsConstructor
public class EsSortFieldBean {

    /**
     * 排序字段
     */
    private String field;

    /**
     * 排序方式  ASC，DESC
     */
    private SortOrder order;

    /**
     * Es sort field bean
     *
     */
    public EsSortFieldBean(String field,SortOrder order) {
        if (StringUtils.isBlank(field)) {
            throw new IllegalArgumentException("order field can not be blank!");
        }

        if (order == null) {
            throw new IllegalArgumentException("order can not be null!");
        }

        this.field = field;
        this.order = order;
    }

}
