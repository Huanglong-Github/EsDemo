package com.hl.es.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hl.es.annotation.EsDocumentId;
import com.hl.es.enums.BulkTypeEnum;
import com.hl.es.util.DateUtil;
import com.hl.es.util.JSONFormat;
import joptsimple.internal.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ExecutionException;


/**
 * @author: huanglong60
 * @date: 2021/1/17 15:00
 * @description:
 */
public abstract class BaseEsService<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseEsService.class);

    /**
     * Index name
     */
    protected String indexName;

    /**
     * 自从es6开始，取消了type，每个index只有一个type，默认为: _doc
     */
    protected String typeName = "_doc";

    /**
     * Es client
     */
    @Autowired
    public Client esClient;

    /**
     * 功能描述： 插入document数据，注意，如果ES数据库中含有对应id的数据，则会更新对应id的文档数据
     * @param t t
     */
    public void createDoc(T t) {
        if (StringUtils.isBlank(indexName)) {
            throw new IllegalArgumentException("index 不能为空！");
        }
        String id = getDocumentId(t);
        IndexResponse indexResponse = null;
        try {
            log.info("create doc data:" + JSONObject.toJSONString(t));
            // 1、创建索引请求
            IndexRequest request = new IndexRequest(
                    indexName,   //索引
                    typeName,     // mapping type
                    id);     //文档id
            request.source(JSONFormat.toJSONString(t, DateUtil.yyyyMMdd_HHmmss), XContentType.JSON);
            //方式一： 用client.index 方法，返回是 ActionFuture<IndexResponse>，再调用get获取响应结果
            indexResponse = esClient.index(request).get();

            //方式二：client提供了一个 prepareIndex方法，内部为我们创建IndexRequest
//            indexResponse = esClient.prepareIndex(indexName, typeName, id)
//                    .setSource(JSONFormat.toJSONString(t, DateUtil.yyyyMMdd_HHmmss), XContentType.JSON)
//                    .get();

            //方式三：request + listener   异步的方式
			ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
			    @Override
			    public void onResponse(IndexResponse indexResponse) {
			        // TODO 响应成功后的处理
                    log.info("更新数据成功");
			    }

			    @Override
			    public void onFailure(Exception e) {
                    // TODO 响应失败后的处理
                    log.info("更新数据失败");
			    }
			};
//            esClient.index(request, listener);

            //5、处理响应
            if(indexResponse != null) {
                String index = indexResponse.getIndex();
                String type = indexResponse.getType();
                String _id = indexResponse.getId();
                long version = indexResponse.getVersion();
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    System.out.println("新增文档成功，处理逻辑代码写到这里。");
                } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    System.out.println("修改文档成功，处理逻辑代码写到这里。");
                }
                // 分片处理信息
                ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

                }
                // 如果有分片副本失败，可以获得失败原因信息
                if (shardInfo.getFailed() > 0) {
                    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        String reason = failure.reason();
                        System.out.println("副本失败原因：" + reason);
                    }
                }
            }
        } catch(ElasticsearchException e) {
            // 捕获，并处理异常
            //判断是否版本冲突、create但文档已存在冲突
            if (e.status() == RestStatus.CONFLICT) {
                log.error("冲突了，请在此写冲突处理逻辑！\n" + e.getDetailedMessage());
            }
            log.error("【添加文档异常】："+"索引名："+ indexName + "写入的数据" + t.toString());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 功能描述：根据对象更新指定文档数据
     * @param t
     */
    public void updateDoc(T t) {
        String id = getDocumentId(t);
        String source = JSONFormat.toJSONString(t, DateUtil.yyyyMMdd_HHmmss_SSS);
        updateDoc(id, source);
    }

    /**
     * 功能描述：根据id和map更新文档数据
     * @param id
     * @param params
     */
    public void updateDoc(String id, Map<String, Object> params) {
        String source = JSONFormat.toJSONString(params, DateUtil.yyyyMMdd_HHmmss_SSS);
        updateDoc(id, source);
    }

    /**
     * 功能描述：根据id和json字符串进行
     * @param id
     * @param source
     */
    private void updateDoc(String id, String source) {
        if (StringUtils.isBlank(indexName)) {
            throw new IllegalArgumentException("update doc, index can not be null");
        }
        try {
            for (int i = 0; i < 3; i++) {
                try {
                    UpdateRequest updateRequest = new UpdateRequest();
                    updateRequest.index(indexName);
                    updateRequest.type(typeName);
                    updateRequest.id(id);
                    updateRequest.doc(source,XContentType.JSON);
                    UpdateResponse updateResponse = esClient.update(updateRequest).get();
                    if(updateResponse!=null){
                        if(updateResponse.getResult()==DocWriteResponse.Result.UPDATED){
                            log.info("index:"+indexName+"type:"+typeName+"id:"+id+"更新结果:成功更新");
                        }
                        if(updateResponse.getResult()==DocWriteResponse.Result.NOOP){
                            log.info("index:"+indexName+"type:"+typeName+"id:"+id+"更新结果:更新成功，但是更新数据和原数据是一样的");
                        }
                    }
                    break;
                } catch (VersionConflictEngineException e) {
                    log.error("更新es冲突,重试");
                    if (i == 2) {
                        throw new RuntimeException("更新es重试三次都冲突");
                    }
                }
            }
        } catch (Exception e) {
            log.error("更新es失败,id:" + id + ",source:" + source, e);
            throw new RuntimeException(e);
        }
    }


    /**
     * 功能描述：bulk,批量添加对象列表到es数据库中
     * @param entities
     */
    public void batchHandleDoc(List<T> entities, BulkTypeEnum bulkTypeEnum){
        Map<String, String> idJsonMap = objectList2Map(entities);
        batchHandleDoc(idJsonMap,bulkTypeEnum);
    }

    /**
     * 功能描述：bulk,添加map对象到es数据库
     * @param idJsonMap
     */
    public void batchHandleDoc(Map<String, String> idJsonMap,BulkTypeEnum bulkTypeEnum) {
        batchHandleDoc(indexName, typeName, idJsonMap,bulkTypeEnum);
    }

    /**
     * 功能描述：bulk,添加map对象到指定index中
     * @param indexName
     * @param typeName
     * @param idJsonMap
     */
    public void batchHandleDoc(String indexName, String typeName, Map<String, String> idJsonMap,BulkTypeEnum bulkTypeEnum){
        BulkRequestBuilder bulkRequest = esClient.prepareBulk();
        for (String id : idJsonMap.keySet()) {
            switch (bulkTypeEnum){
                case BULK_CREATE:
                // 批量添加
                    IndexRequestBuilder request = esClient.prepareIndex(indexName, typeName, id);
                    String source = idJsonMap.get(id);
                    request.setSource(source, XContentType.JSON);
                    bulkRequest.add(request);
                    break;
                case BULK_UPDATE:
                // 批量更新
                    UpdateRequestBuilder request1 = esClient.prepareUpdate(indexName, typeName, id);
                    String source1 = idJsonMap.get(id);
                    request1.setDoc(source1,XContentType.JSON);
                    bulkRequest.add(request1);
                    break;
                case BULK_DELETE:
                // 批量删除
                    DeleteRequestBuilder request2 = esClient.prepareDelete(indexName, typeName, id);
                    bulkRequest.add(request2);
                    break;
            }
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            throw new RuntimeException("插入es失败," + bulkResponse.buildFailureMessage() + ",数据:" + idJsonMap);
        } else {
            String errorInfo = "批量插入Es成功IndexName-%s,TypeName-%s,IdJsonMap-%s";
            log.error(String.format(errorInfo, indexName, typeName, (idJsonMap != null && idJsonMap.size() > 0 ? idJsonMap.toString() : "")));
        }
    }

    /**
     * 功能描述：根据自定义注解EsDocumentId，反射得到index的id
     * @param t
     * @return
     */
    private String getDocumentId(T t) {
        Field[] fields = t.getClass().getDeclaredFields();
        Object id = null;
        for (Field field : fields) {
            EsDocumentId esDocumentId = field.getAnnotation(EsDocumentId.class);
            if (esDocumentId != null) {
                try {
                    field.setAccessible(true);
                    id = field.get(t);
                } catch (IllegalAccessException e) {
                    // do nothing
                }
                if (id != null && StringUtils.isNotBlank(id.toString())) {
                    return id.toString();
                }
            }
        }
        throw new IllegalArgumentException("not found document id of " + t.getClass().getName());
    }

    /**
     * 功能描述：根据id和class得到指定的document数据
     * @param id
     * @param clazz
     * @return
     */
    public T getById(String id,
                     Class<T> clazz) {
        GetResponse response = esClient.prepareGet(indexName, typeName, id).get();
        if (response.isExists()) {
            String source = response.getSourceAsString();
            return JSON.parseObject(source, clazz);
        }
        return null;
    }

    /**
     * 功能描述：根据GetRequest和class得到指定的document数据
     * @param clazz
     * @param request
     * @return
     */
    public T getById(Class<T> clazz, GetRequest request) {
        GetResponse response = null;
        try {
            response = esClient.get(request).get();
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.error("没有找到该id的文档");
            }
            if (e.status() == RestStatus.CONFLICT) {
                log.error("获取时版本冲突了，请在此写冲突处理逻辑！");
            }
            log.error("获取文档异常", e);
        } catch (InterruptedException | ExecutionException e) {
            log.error("索引异常", e);
        }
        String source = Strings.EMPTY;
        //4、处理响应
        if(response != null) {
            String index = response.getIndex();
            String type = response.getType();
            String id = response.getId();
            if (response.isExists()) { // 文档存在
                long version = response.getVersion();
                source = response.getSourceAsString(); //结果取成 String
//                Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();  // 结果取成Map
//                byte[] sourceAsBytes = getResponse.getSourceAsBytes();    //结果取成字节数组
                log.info("index:" + index + "  type:" + type + "  id:" + id);
                log.info(source);
            } else {
                log.error("没有找到该id的文档" );
            }
        }
        return JSON.parseObject(source, clazz);
    }

    /**
     * 功能描述：根据文档的id数组得到文档数据集合
     * @param ids
     * @return
     */
    public List<T> multiGetById(List<String> ids,Class<T> clazz){
        List<T> result = new ArrayList<>();
        MultiGetRequestBuilder multiGetRequestBuilder = esClient.prepareMultiGet();
        for(String id:ids){
            multiGetRequestBuilder.add(indexName, typeName, id);
        }
        MultiGetResponse multiGetItemResponses = multiGetRequestBuilder.get();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String source = response.getSourceAsString();
                T t = JSON.parseObject(source, clazz);
                result.add(t);
            }
        }
        return result;
    }

    /**
     * 功能描述：根据QueryBuilder查询分页,, queryHelper参数只使用排序及分页信息
     *
     * @param queryHelper  query helper
     * @param clazz        clazz
     * @return es result
     */
    public EsResult<T> searchPageByQueryBuilder(EsQueryHelper queryHelper,Class<T> clazz) {
        try {
            SearchRequestBuilder searchRequestBuilder = esClient.prepareSearch(indexName).setTypes(typeName)
                    .setQuery(queryHelper.getQueryBuilder());
            EsResult<T> result = new EsResult(queryHelper.getPageIndex(), queryHelper.getPageSize());
            searchRequestBuilder.setFrom(result.getStartIndex()).setSize(result.getPageSize());

            //设置Scroll的时间，可根据实际需要进行调整
            searchRequestBuilder.setScroll(new TimeValue(60000));
            //组装排序条件
            wrapSortQuery(queryHelper, searchRequestBuilder);
            //开始搜索
            SearchResponse searchResponse = searchRequestBuilder.get();
            //组装查询结果
            wrapResutHit(result, searchResponse, clazz);
            return result;
        } catch (Throwable e) {
            log.error("elastic searchPage error", e);
            throw new RuntimeException("elastic searchPage error", e);
        }
    }

    /**
     * 功能描述；删除指定index的指定id文档
     * @param id
     */
    public void delById(String id){
        DeleteResponse response = esClient.prepareDelete(indexName, typeName, id).get();
        if(response != null) {
            if(response.status() == RestStatus.OK){
                log.info("[indexName]"+indexName+"[typeName]"+typeName+"[id]"+id+"删除成功");
            }else if(response.status() == RestStatus.NOT_FOUND){
                log.info("[indexName]"+indexName+"[typeName]"+typeName+"[id]"+id+"不存在，无法删除");
            }else {
                log.info("[indexName]"+indexName+"[typeName]"+typeName+"[id]"+id+"删除失败");
            }
        }
    }

    /**
     * 功能描述：查询指定文档并删除
     * @param queryBuilderList
     */
    public void delByQuery(List<QueryBuilder> queryBuilderList){
        DeleteByQueryRequestBuilder deleteRequestBuilder = new DeleteByQueryRequestBuilder(esClient, DeleteByQueryAction.INSTANCE);
        deleteRequestBuilder.source(indexName);
        if(!queryBuilderList.isEmpty()){
            for(QueryBuilder queryBuilder:queryBuilderList){
                deleteRequestBuilder.filter(queryBuilder);
            }
        }
        BulkByScrollResponse response = deleteRequestBuilder.get();

        //这里可以对删除的结果进行对应的处理
        long deleted = response.getDeleted();
        log.info("[indexName]"+indexName+"[typeName]"+"查询并删除成功"+deleted);

    }

    /**
     * 功能描述：查询指定文档并删除  异步方式
     * @param queryBuilderList
     */
    public void delByQueryAsyn(List<QueryBuilder> queryBuilderList){
        DeleteByQueryRequestBuilder deleteRequestBuilder = new DeleteByQueryRequestBuilder(esClient, DeleteByQueryAction.INSTANCE);
        if(!queryBuilderList.isEmpty()){
            for(QueryBuilder queryBuilder:queryBuilderList){
                deleteRequestBuilder.filter(queryBuilder);
            }
        }
        deleteRequestBuilder.source(indexName);
        deleteRequestBuilder.execute(new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse response) {
                //这里可以对删除的结果进行对应的处理
                long deleted = response.getDeleted();
                log.info("[异步]"+"[indexName]"+indexName+"[typeName]"+"查询并删除成功"+deleted);
            }

            @Override
            public void onFailure(Exception e) {
                // Handle the exception
                log.info("[异步]"+"[indexName]"+indexName+"[typeName]"+"查询并删除成功");
            }
        });
    }

    /**
     * 功能描述：组装排序查询条件
     *
     * @param queryHelper          query helper
     * @param searchRequestBuilder search request builder
     */
    private void wrapSortQuery(EsQueryHelper queryHelper, SearchRequestBuilder searchRequestBuilder) {
        if (queryHelper.hasSort()) {
            for (EsSortFieldBean sortField : queryHelper.getSortFields()) {
                searchRequestBuilder.addSort(sortField.getField(), sortField.getOrder());
            }
        }
    }

    /**
     * 功能描述；组装查询结果
     * @param result         result
     * @param searchResponse search response
     * @param clazz          clazz
     */
    private void wrapResutHit(EsResult<T> result,
                              SearchResponse searchResponse,
                              Class<T> clazz) {
        SearchHits hits = searchResponse.getHits();

        TotalHits totalHits = hits.getTotalHits();

        List<T> data = new ArrayList<>();
        for (SearchHit hit : hits.getHits()) {
            String source = hit.getSourceAsString();
            T t = JSON.parseObject(source, clazz);
            data.add(t);
        }
        result.setTotalCount((int) totalHits.value);
        result.setData(data);
    }


    /**
     * 功能描述：根据查询条件和指定时间维度进行聚合统计
     * TODO 这里统一将聚合统计数据用map<String,String>返回，其实map的value会有不同的数据类型，如有需要，可在数据返回后进行类型转换
     * @param column
     * @param queryHelper
     * @param dateHistogramInterval
     */
    public Map<String,String> histogramAgg(String column, EsQueryHelper queryHelper,DateHistogramInterval dateHistogramInterval) {
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = AggregationBuilders.dateHistogram(column)
                .field(column)
                .minDocCount(1) //过滤一下，最小的统计数据为1
                .calendarInterval(dateHistogramInterval);
        SearchResponse response = esClient.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(queryHelper.getQueryBuilder())
                .addAggregation(dateHistogramAggregationBuilder)
                .get();
        Histogram agg = response.getAggregations().get(column);
        List<? extends Histogram.Bucket> buckets = agg.getBuckets();
        Map<String, String> map = new HashMap<>();
        buckets.stream().forEach((bucket)->{
            Long value = ((Histogram.Bucket) bucket).getDocCount();
            map.put(((Histogram.Bucket) bucket).getKeyAsString(),value.toString());
            log.info("[indexName]"+indexName+"[typeName]"+"根据"+column+"进行时间维度统计："+(((Histogram.Bucket) bucket).getKeyAsString()+":"+((Histogram.Bucket) bucket).getDocCount()));
        });
        return map;
    }



    /**
     * 功能描述：范围分组区间统计
     * TODO 这里统一将聚合统计数据用map<String,String>返回，其实map的value会有不同的数据类型，如有需要，可在数据返回后进行类型转换
     * @param column
     * @param queryHelper
     */
    public Map<String,String> rangeAgg(String column, EsQueryHelper queryHelper,double[][] rangeArray) {
        RangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders.range("rangeAgg").field(column);
        //设置查询范围分组
        if(Objects.nonNull(rangeArray)){
            if(rangeArray.length<2){
                log.info("[indexName]"+indexName+"[typeName]"+"根据"+column+"进行范围维度统计的范围设置不正确！！！"+"time:"+DateUtil.getCurrentTimeMillis());
                // TODO 这里可添加其他处理逻辑
                return null;
            }
            for (int i=0;i<rangeArray.length;i++){
                if(rangeArray[i].length==2){
                    rangeAggregationBuilder.addRange(rangeArray[i][0],rangeArray[i][1]);
                } else {
                    if(i==0){
                        rangeAggregationBuilder.addUnboundedTo(rangeArray[0][0]);
                    } else {
                        rangeAggregationBuilder.addUnboundedFrom(rangeArray[i][0]);
                    }
                }
            }
        }
        SearchResponse response = esClient.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(queryHelper.getQueryBuilder())
                .addAggregation(rangeAggregationBuilder)
                .get();
        Map<String, String> map = new HashMap<>();
        Aggregations aggregations = response.getAggregations();
        Range range = aggregations.get("rangeAgg");
        List<? extends Range.Bucket> buckets = range.getBuckets();
        //遍历聚合结果
        buckets.stream().forEach((bucket)->{
            // 获取桶的Key值
            String key = bucket.getKeyAsString();
            // 获取文档总数
            Long count = bucket.getDocCount();
            map.put(key,count.toString());
            log.info("[indexName]"+indexName+"根据"+column+"进行范围维度统计："+"key："+key+"docCount："+count);
        });
        return map;
    }





    /**
     * 功能描述：分组统计，统计次数
     * @param column
     * @param queryHelper
     * @return
     */
    public Map<String,String> termsAgg(String column, EsQueryHelper queryHelper) {
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(column)
                .field(column)
                .minDocCount(1);//过滤一下，最小的统计数据为1
        SearchResponse response = esClient.prepareSearch(indexName)
                .setTypes(typeName)
                .setSize(0) //设置查询_source为0
                .setQuery(queryHelper.getQueryBuilder())
                .addAggregation(termsAggregationBuilder)
                .get();
        Terms agg = response.getAggregations().get(column);
        List<? extends Terms.Bucket> buckets = agg.getBuckets();
        Map<String, String> map = new HashMap<>();
        buckets.stream().forEach((bucket)->{
            Long value = bucket.getDocCount();
            map.put(bucket.getKeyAsString(),value.toString());
            log.info("[indexName]"+indexName+"[typeName]"+"根据"+column+"分组统计:"+bucket.getKeyAsString()+":"+ bucket.getDocCount());
        });
        return map;
    }

    /**
     * 功能描述；去重统计，统计个数
     * @param groupColumn
     * @param queryHelper
     * @return
     */
    public long countByDistinct(String groupColumn, EsQueryHelper queryHelper) {

        CardinalityAggregationBuilder cardinalityAggregationBuilder = AggregationBuilders.cardinality("count")
                .field(groupColumn);
        SearchResponse response = esClient.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(queryHelper.getQueryBuilder())
                .addAggregation(cardinalityAggregationBuilder)
                .setSize(0)
                .get();
        Cardinality cardinality = response.getAggregations().get("count");
        log.info("[indexName]"+indexName+"[typeName]"+"根据"+groupColumn+"去重统计,"+"值为："+cardinality.getValue());
        return cardinality.getValue();
    }


    /**
     * 功能描述：统计查询，对指定数值字段进行数据统计，最小值，最大值，平均值，总和，出现次数
     * @param groupColumn
     * @param queryHelper
     * @return
     */
    public Map<String,String> stat(String groupColumn, EsQueryHelper queryHelper){
        StatsAggregationBuilder aggregation =
                AggregationBuilders
                        .stats("agg")
                        .field(groupColumn);
        SearchResponse response = esClient.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(queryHelper.getQueryBuilder())
                .addAggregation(aggregation)
                .setSize(0)
                .get();
        Stats agg = response.getAggregations().get("agg");
        Double min = agg.getMin();
        Double max = agg.getMax();
        Double avg = agg.getAvg();
        Double sum = agg.getSum();
        Long count = agg.getCount();//注意，这里的count并没有去重
        Map<String,String> map = new HashMap<>();
        map.put("min",min.toString());
        map.put("max",max.toString());
        map.put("avg",avg.toString());
        map.put("sum",sum.toString());
        map.put("count",count.toString());
        log.info("[indexName]"+indexName+"[typeName]"+"根据"+groupColumn+"进行数据统计,"+"值为："+JSON.toJSONString(map));
        return map;
    }

    /**
     * 功能描述：子聚合统计查询，这里封装的很简陋，暂时没想到更好的封装方式
     * @param queryHelper
     * @param groupAggBuilder
     * @return
     */
    public SearchResponse subAgg(EsQueryHelper queryHelper,AggregationBuilder groupAggBuilder){
        Map<String,String> map = new HashMap<>();

        //组装排序规则
        FieldSortBuilder fieldSortBuilder = new FieldSortBuilder("money_sum");
        fieldSortBuilder.order(SortOrder.ASC);
        List<FieldSortBuilder> sortBuilders = new ArrayList<>();
        sortBuilders.add(fieldSortBuilder);

        //对聚合结果进行分页处理
        groupAggBuilder.subAggregation(new BucketSortPipelineAggregationBuilder("bucket_field",sortBuilders)
                .from(queryHelper.getStartIndex()).size(queryHelper.getPageSize()));

        SearchResponse response = esClient.prepareSearch(indexName).setTypes(typeName)
//                .setRouting("xxx")  // 若已知数据属于某一个或几个路由分区，设置路由会提升性能。
                .setSize(0)
                .setQuery(queryHelper.getQueryBuilder())
                .addAggregation(groupAggBuilder)
                .get();

        return response;
    }

    /**
     * 功能描述：将实体对象列表转换为hashMap
     * @param entities
     * @return
     */
    public Map<String, String> objectList2Map(List<T> entities){
        Map<String, String> idJsonMap = new HashMap<>();
        for (T t : entities) {
            String id = getDocumentId(t);
            String source = JSONFormat.toJSONString(t, DateUtil.yyyyMMdd_HHmmss_SSS);
            idJsonMap.put(id, source);
        }
        return idJsonMap;
    }

    /**
     * @return indexName
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * 设置indexName
     * @param indexName
     */
    protected abstract void setIndexName(String indexName);

    /**
     * @return typeName
     */
    public String getTypeName() {
        return typeName;
    }
}
