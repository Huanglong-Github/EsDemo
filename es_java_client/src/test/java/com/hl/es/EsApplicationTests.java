package com.hl.es;

import com.hl.es.enitiy.TrainOrder;
import com.hl.es.enums.BulkTypeEnum;
import com.hl.es.service.EsResult;
import com.hl.es.service.EsTrainOrderService;
import com.hl.es.util.DateUtil;
import com.hl.es.util.JSONFormat;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.Client;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

@SpringBootTest
@Slf4j
class EsApplicationTests {

    @Test
    void contextLoads() {
        TrainOrder trainOrder = TrainOrder.builder().account("18370053196").fromStationName("北京西").created(new Date()).totalPayFee(123L).build();
        String s = JSONFormat.toJSONString(trainOrder, DateUtil.DEFAULT_DATE_FORMATE);
        log.info(s);
    }


    @Autowired
    Client esClient;

    @Test
    void stringTest(){
        String address = "192.168.197.129:9300";
        String[] split = address.split(",");
        System.out.println(split.length);
        System.out.println(split[0]);
        System.out.println();
    }

    @Autowired
    EsTrainOrderService esTrainOrderService;


    //    添加文档或修改文档
    @Test
    void create(){
        TrainOrder trainOrder = TrainOrder.builder().orderId(6L).totalPayFee(5L).created(new Date()).account("黄老邪").build();
        esTrainOrderService.createTrainOrderDoc(trainOrder);
    }

    //   更新文档数据
    @Test
    void update(){
        Long id = 1L;
        TrainOrder trainOrder = TrainOrder.builder().orderId(id).fromStationName("广州站").build();
        esTrainOrderService.updateTrainOrderDoc(trainOrder);
    }

    // 批量处理数据
    @Test
    void batchHandle(){
        List<TrainOrder> trainOrderList = new ArrayList<>();
        TrainOrder trainOrder = TrainOrder.builder().orderId(3L).totalPayFee(20L).created(new Date()).account("黄小龙龙").build();
        TrainOrder trainOrder1 = TrainOrder.builder().orderId(4L).totalPayFee(20L).created(new Date()).account("黄小龙龙").build();
        trainOrderList.add(trainOrder);
        boolean add = trainOrderList.add(trainOrder1);
        esTrainOrderService.batchHandleTrainOrderDoc(trainOrderList, BulkTypeEnum.BULK_CREATE);
    }

    // 获取文档
    @Test
    void getDocumentById(){
        TrainOrder trainOrderById = esTrainOrderService.getTrainOrderById("11");
        System.out.println(trainOrderById.toString());
    }

    @Test
    void getMultiDocById(){
        List<String> ids = new ArrayList<>();
        ids.add("10");
        ids.add("11");
        ids.add("12");
        List<TrainOrder> trainOrders = esTrainOrderService.multiGetTrainOrderById(ids);
        if(!trainOrders.isEmpty()){
            for(TrainOrder trainOrder:trainOrders){
                System.out.println(trainOrder.toString());
            }
        }
    }

    //删除文档

    @Test
    void delDocumentById(){
        esTrainOrderService.delByTrainOrderId("1");
    }

    /**
     * 根据查询条件查询并删除指定文档 同步方式
     */
    @Test
    void delByQuery(){
        esTrainOrderService.delTrainOrderByQuery();
    }


    /**
     * 根据查询条件查询并删除指定文档  异步方式
     */
    @Test
    void delByQueryAsyn(){
        esTrainOrderService.delTrainOrderByQueryAsyn();
    }




    @Test
    void searchPage(){
        EsResult<TrainOrder> trainOrderEsResult = esTrainOrderService.searchTrainOrderPageByQueryBuilder();
        if(!trainOrderEsResult.getData().isEmpty()){
            for(TrainOrder trainOrder:trainOrderEsResult.getData()){
                System.out.println(trainOrder.toString());
            }
        }
    }


    //聚合统计
    @Test
    void aggTest(){
        //在时间维度进行聚合统计
        //esTrainOrderService.trainOrderDateHistogramAgg();

        //terms 分组统计
        //esTrainOrderService.trainOrderTermsAgg();

        //去重统计
        //esTrainOrderService.countAccountByDistinct();

        //对trainOrder数据的totalPayFee字段进行各类统计
        //esTrainOrderService.trainOrderStat();

        //分页统计
        //esTrainOrderService.aggTrainOrderPage();


        //range 范围统计
        esTrainOrderService.trainOrderRangeAgg();

    }



    @Test
    public void queryBuilderTest(){
        //单个queryBuilder测试
     //   esTrainOrderService.matchQueryDemo();

        //多个queryBuilder测试
        esTrainOrderService.manyQueryDemo();

    }







    @Test
    public void test(){
        double[][] rangeArray = {{0,5},{5}};
//        double[][] rangeArray = {{5},{5,10},{10,30},{30}};

//        double[][] rangeArray = {{30,100}};
        if(Objects.nonNull(rangeArray)){
            if(rangeArray.length<2){
                return;
            }
            for (int i=0;i<rangeArray.length;i++){
                if(rangeArray[i].length==2){
                    System.out.println(rangeArray[i][0]+":"+rangeArray[i][1]);
                } else {
                    if(i==0){
                        System.out.println(rangeArray[0][0]);
                    } else {
                        System.out.println(rangeArray[i][0]);
                    }
                }
            }
        }
    }

}
