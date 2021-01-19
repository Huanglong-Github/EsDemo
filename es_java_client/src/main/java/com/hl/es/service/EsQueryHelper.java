package com.hl.es.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

/**
 * @author: huanglong60
 * @date: 2021/1/17 23:19
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EsQueryHelper {
    /**
     * 查询条件
     */
    QueryBuilder queryBuilder = null;
    /**
     * Page index
     */
    private int pageIndex = 1;
    /**
     * Page size
     */
    private int pageSize = 10;

    /**
     * 排序字段
     */
    private List<EsSortFieldBean> sortFields;


    /**
     * 功能描述：判断是否有排序字段
     * @return
     */
    public boolean hasSort() {
        if (sortFields != null && sortFields.size() > 0) {
            return true;
        }
        return false;
    }


    /**
     * 功能描述：返回分页查询的起始位置
     * @return
     */
    public int getStartIndex() {
        return (pageIndex - 1) * pageSize;
    }

}
