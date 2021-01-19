package com.hl.es.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hl.es.enitiy.TrainOrder;
import com.hl.es.enums.BulkTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.util.CollectionUtil;
import org.checkerframework.dataflow.qual.TerminatesExecution;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * @author: huanglong60
 * @date: 2021/1/17 15:49
 * @description:
 */

@Service("esTrainOrderService")
@Slf4j
public class EsTrainOrderService extends BaseEsService<TrainOrder>{

    @Value("train_order")
    @Override
    protected void setIndexName(String indexName) {
        this.indexName = indexName;
    }


    /**
     * 功能描述：添加对象
     */
    public void createTrainOrderDoc(TrainOrder trainOrder){
        //这里方便测试，直接使用外部的trainOrder实例进行添加数据，
        // 实际上，这个函数可以根据需要添加入参，并进行数据组装成trainOrder,
        // 然后再执行插入到ES数据库中的操作
        createDoc(trainOrder);
    }

    /**
     * 功能描述：批量添加trainOrder对象列表到ES数据库
     * @param trainOrderList
     */
    public void batchHandleTrainOrderDoc(List<TrainOrder> trainOrderList, BulkTypeEnum bulkTypeEnum){
        batchHandleDoc(trainOrderList,bulkTypeEnum);
    }

    /**
     * 功能描述：根据id查询得到trainOrder文档数据
     * @return
     */
    public TrainOrder getTrainOrderById(String id){
        GetRequest request = new GetRequest(
                indexName,   //索引
                typeName,     // mapping type
                id);     //文档id

        // 2、可选的设置
        //request.routing("routing");
        //request.version(2);

        //request.fetchSourceContext(new FetchSourceContext(false)); //是否获取_source字段,这里一般都要获取，聚合查询的时候，可设置为不获取

        //选择返回的字段
//        String[] includes = new String[]{"message", "*Date"};
//        String[] excludes = Strings.EMPTY_ARRAY;
//        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
//        request.fetchSourceContext(fetchSourceContext);

//        return getById(TrainOrder.class, request);
        return getById(id,TrainOrder.class);
    }

    /**
     * 功能描述：根据id集合，得到所对应的文档数据集合
     * @param ids
     * @return
     */
    public List<TrainOrder> multiGetTrainOrderById(List<String> ids){
        return multiGetById(ids,TrainOrder.class);
    }


    /**
     * 功能描述：根据id删除document数据
     * @param id
     */
    public void delByTrainOrderId(String id){
        delById(id);
    }


    /**
     * 功能描述：查询指定文档并删除
     */
    public void delTrainOrderByQuery(){
        List<QueryBuilder> queryBuilderList = new ArrayList<>();
        QueryBuilder matchQueryBuilder = QueryBuilders.termQuery("orderId", "1");
        queryBuilderList.add(matchQueryBuilder);
        delByQuery(queryBuilderList);
    }


    /**
     * 功能描述：查询指定文档并删除 同步方式
     */
    public void delTrainOrderByQueryAsyn(){
        List<QueryBuilder> queryBuilderList = new ArrayList<>();
        QueryBuilder matchQueryBuilder = QueryBuilders.termQuery("orderId", "1");
        queryBuilderList.add(matchQueryBuilder);
        delByQueryAsyn(queryBuilderList);
    }


    /**
     * TODO 根据函数的实际调用，可为这个函数添加入参，组装后再进行查询
     * @return
     */
    public EsResult<TrainOrder> searchTrainOrderPageByQueryBuilder(){
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        EsQueryHelper queryHelper = new EsQueryHelper();
        //组装排序规则
        EsSortFieldBean esSortFieldBean = new EsSortFieldBean();
        esSortFieldBean.setField("created");
        esSortFieldBean.setOrder(SortOrder.DESC);
        List<EsSortFieldBean> sortFields = new ArrayList<>();
        sortFields.add(esSortFieldBean);
        queryHelper.setSortFields(sortFields);
        queryHelper.setQueryBuilder(matchAllQueryBuilder);
        EsResult<TrainOrder> trainOrderEsResult = searchPageByQueryBuilder(queryHelper, TrainOrder.class);
        return trainOrderEsResult;
    }


    /**
     * 功能描述：根据时间维度对订单数量进行统计
     * TODO 这个函数可以根据需要添加入参，组装查询条件
     */
    public void trainOrderDateHistogramAgg(){
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        EsQueryHelper queryHelper = new EsQueryHelper();
        queryHelper.setQueryBuilder(matchAllQueryBuilder);
        histogramAgg("created",queryHelper, DateHistogramInterval.YEAR);
    }


    /**
     * 功能描述：分组统计在指定查询条件下，符合查询条件的每个账号有多少订单
     * TODO 这个函数可以根据需要添加入参，组装查询条件
     */
    public void trainOrderTermsAgg(){
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        EsQueryHelper queryHelper = new EsQueryHelper();
        queryHelper.setQueryBuilder(matchAllQueryBuilder);
        termsAgg("account",queryHelper);
    }


    /**
     * 功能描述：在指定查询条件下，统计共有多少人下单，去重
     */
    public void countAccountByDistinct(){
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        EsQueryHelper queryHelper = new EsQueryHelper();
        queryHelper.setQueryBuilder(matchAllQueryBuilder);
        countByDistinct("account",queryHelper);
    }

    /**
     * 功能描述：在指定的查询条件下，对trainOrder数据的totalPayFee字段进行各类统计
     */
    public void trainOrderStat(){
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        EsQueryHelper queryHelper = new EsQueryHelper();
        queryHelper.setQueryBuilder(matchAllQueryBuilder);
        stat("totalPayFee",queryHelper);
    }

    /**
     * 功能描述：对聚合结果进行分页处理
     * TODO 这个方法根据实际需要，编写对应的聚合条件进行聚合
     */
    public void aggTrainOrderPage(){

        AggregationBuilder groupAggBuilder = AggregationBuilders.terms("account_term")
                .field("account")
                .size(10)
                .executionHint("map");  // 若可知该层聚合结果数量很小，设置成map可提升性能

        //创建子聚集条件
        AggregationBuilder topScoreAggBuilder = AggregationBuilders.avg("money_avg").field("totalPayFee");
        AggregationBuilder topScoreAggBuilder1 = AggregationBuilders.sum("money_sum").field("totalPayFee");

        //创建hitTop查询
        AggregationBuilder topHitsAggBuilder= AggregationBuilders.topHits("top")
                .explain(true)  //开始explain分析，根据情况决定是否开启
                .sort("created", SortOrder.DESC)   //根据指定字段进行排序，这里的字段只能是train_order里面的字段,这里选择时间字段
                .from(0)
                .size(2);

        //添加子聚集
        groupAggBuilder.subAggregation(topScoreAggBuilder);
        groupAggBuilder.subAggregation(topScoreAggBuilder1);
        groupAggBuilder.subAggregation(topHitsAggBuilder);

        //构建查询builder
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        EsQueryHelper queryHelper = new EsQueryHelper();
        queryHelper.setQueryBuilder(matchAllQueryBuilder);
        queryHelper.setPageIndex(1);
        queryHelper.setPageSize(10);

        SearchResponse response = subAgg(queryHelper, groupAggBuilder);
        Terms aggregation = response.getAggregations().get("account_term");
//        List<? extends Terms.Bucket> buckets = aggregation.getBuckets();
        if(Objects.nonNull(aggregation)){
            for(Terms.Bucket bucket:aggregation.getBuckets()){
                System.out.println("==========子聚合 & 分页查询===================");
                String key = bucket.getKeyAsString();
                System.out.println("key:"+key);
                long docCount = bucket.getDocCount();
                System.out.println("docCount:"+docCount);
                Avg avg = (Avg)bucket.getAggregations().asMap().get("money_avg");
                System.out.println("avg:"+avg.getValue());
                Sum sum = (Sum)bucket.getAggregations().asMap().get("money_sum");
                System.out.println("sum:"+sum.getValue());
                TopHits topHits = bucket.getAggregations().get("top");
                SearchHit[] hits = topHits.getHits().getHits();

                for(SearchHit hit:hits){
                    TrainOrder trainOrder = JSON.parseObject(hit.getSourceAsString(), TrainOrder.class);
                    System.out.println("trainOrder:"+trainOrder.toString());
                }
            }
        }
    }



    /**
     * 各类查询条件小计
     */
    public void queryBuilderList(){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(boolQueryBuilder);//这里需要填写一个QueryBuilder
        ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery("fieldName");
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("fieldName").from(0).to(20);
        FuzzyQueryBuilder fuzzyQueryBuilder = QueryBuilders.fuzzyQuery("fieldName", "value");
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("fieldName", "value");
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("fieldName", "value");
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("fieldName", "value");
        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = QueryBuilders.matchPhrasePrefixQuery("fieldName", "value");
        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("fieldName", "value");
        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("feildName", "value");
    }

    public void matchQueryDemo(){
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("fromStationName", "北京");
        EsQueryHelper esQueryHelper = new EsQueryHelper();
        esQueryHelper.setQueryBuilder(matchQueryBuilder);
        EsResult<TrainOrder> trainOrderEsResult = searchPageByQueryBuilder(esQueryHelper, TrainOrder.class);
        if(!trainOrderEsResult.getData().isEmpty()){
            for(TrainOrder trainOrder:trainOrderEsResult.getData()){
                log.info("matchQueryDemo:"+trainOrder.toString());
            }
        }
    }

}
