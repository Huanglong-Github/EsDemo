package com.hl.es.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: huanglong60
 * @date: 2021/1/17 23:48
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EsResult<T> {
    /**
     * Page index
     */
    private int pageIndex;
    /**
     * Page size
     */
    private int pageSize;
    /**
     * Total count
     */
    private int totalCount;

    /**
     * Data
     */
    private List<T> data = new ArrayList<>();

    public EsResult(int pageIndex, int pageSize) {
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }

    public int getStartIndex() {
        return (pageIndex - 1) * pageSize;
    }


}
