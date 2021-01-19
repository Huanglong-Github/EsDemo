package com.hl.es.enitiy;

import com.alibaba.fastjson.annotation.JSONField;
import com.hl.es.annotation.EsDocumentId;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

/**
 * @author: huanglong60
 * @date: 2021/1/14 18:23
 * @description: 简化版的订单
 */

@Data
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TrainOrder {
    /**
     * 本地订单id
     */
    @EsDocumentId
    private Long orderId;
    /**
     * 12306账号
     */
    private String account;
    /**
     * 出发站名称
     */
    private String fromStationName;
    /**
     * 到达站名称
     */
    private String toStationName;
    /**
     * 实付订单总金额
     */
    private Long totalPayFee;
    /**
     * 创建时间
     */
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date created;
    /**
     * 订单类型<br/>
     * REAL_BOOK(0,"实占","实"),
     * VIRTUAL_BOOK(1,"虚占","虚"),
     * GRAB_TICKET(2,"抢票","抢"),
     * CHANGES_TICKET(3,"改签","改"),
     * STUDENT_TICKET(4,"学生票","学生票"),
     * DELIVERY_TICKET(5,"配送票","配送票")
     */
    private Integer orderType;
    /**
     * 是否购买保险  0没有购买  1购买
     */
    private Integer isPurchaseInsurance;
}
