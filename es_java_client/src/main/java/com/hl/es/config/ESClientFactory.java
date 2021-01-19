package com.hl.es.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: huanglong60
 * @date: 2021/1/14 18:22
 * @description: 借助FactoryBean组建client
 */
@Slf4j
@Service("esClient")
public class ESClientFactory implements FactoryBean<Client>, InitializingBean, DisposableBean {

    private Client client = null;

    @Value("${es.train.cluster}")
    private String clusterName;

    @Value("${es.train.addresses}")
    private String addressesStr;//地址

    @Value("${es.train.cluster}")
    private String UserName;

    @Value("${es.train.cluster}")
    private String Password;


    @Override
    public void afterPropertiesSet() throws Exception {
        client = this.buildTransportClient();
    }

    /**
     * 功能描述：构建transportClient
     * @return
     */
    private Client buildTransportClient() {
        log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>build es client>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        log.info("clusterName = " + clusterName);
        log.info("addressesStr = " + addressesStr);
        log.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<build es client<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        List<TransportAddress> addresses = buildTransportAddresses();

        //设置集群的名字
        Settings settings = Settings.builder()
                //.put("cluster.name", clusterName) //如果集群的名字不是默认的elasticsearch，需指定
                .put("client.transport.sniff", false) //自动嗅探
//                .put("request.headers.Authorization",basicAuthHeaderValue(UserName, Password))
                .build();

        TransportClient transportClient = new PreBuiltTransportClient(settings);
        for (TransportAddress address : addresses) {
            transportClient.addTransportAddresses(address);
        }
        return transportClient;
    }

    /**
     * 功能描述：组装ES数据库连接地址
     * @return
     */
    private List<TransportAddress> buildTransportAddresses() {
        String[] addressStrArr = addressesStr.split(",");
        List<TransportAddress> addresses = new ArrayList<>();
        try {
            for (String addressStr : addressStrArr) {
                String[] ipPortStr = addressStr.split(":");
                addresses.add(new TransportAddress(InetAddress.getByName(ipPortStr[0]),
                        Integer.parseInt(ipPortStr[1])));
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException("解析ES连接地址异常.addressesStr:" + addressesStr, e);
        }
        return addresses;
    }


    /**
     * 功能描述：基础的base64生成 JDK使用1.8以下的，可自行找相应的base64工具
     *
     * @param username 用户名
     * @param passwd   密码
     * @return string
     */
    private static String basicAuthHeaderValue(String username, String passwd) {
        CharBuffer chars = CharBuffer.allocate(username.length() + passwd.length() + 1);
        byte[] charBytes = null;
        try {
            chars.put(username).put(':').put(passwd.toCharArray());
            charBytes = toUtf8Bytes(chars.array());
            String basicToken = new String(Base64.encodeBase64(charBytes));
            return "Basic " + basicToken;
        } finally {
            Arrays.fill(chars.array(), (char) 0);
            if (charBytes != null) {
                Arrays.fill(charBytes, (byte) 0);
            }
        }
    }

    /**
     * 功能描述：utf-8加密字符数组
     * @param chars
     * @return
     */
    public static byte[] toUtf8Bytes(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        byte[] bytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        Arrays.fill(byteBuffer.array(), (byte) 0);
        return bytes;
    }

    @Override
    public Client getObject() throws Exception {
        return this.client;
    }

    @Override
    public Class<?> getObjectType() {
        return Client.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void destroy() throws Exception {
        try {
            if (this.client != null) {
                this.client.close();
            }
        } catch (Exception e) {
            log.error("ES关闭client异常", e);
        }
    }

    /**
     * 功能描述：设置es集群名称
     *
     * @param clusterName cluster name
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * 功能描述：设置es集群地址
     * @param addressesStr addresses str
     */
    public void setAddressesStr(String addressesStr) {
        this.addressesStr = addressesStr;
    }
}

