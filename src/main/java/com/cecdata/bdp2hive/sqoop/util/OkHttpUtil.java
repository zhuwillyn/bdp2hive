package com.cecdata.bdp2hive.sqoop.util;

import com.alibaba.fastjson.JSONObject;
import okhttp3.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * @author zhuweilin
 * @project cecdata-platform
 * @description http请求客户端，主要是模拟发送http请求
 * @mail zhuwillyn@163.com
 * @date 2017/12/27 11:36
 */
public class OkHttpUtil {

    public static Response get(String url) throws IOException {
        if (org.apache.commons.lang.StringUtils.isNotEmpty(url)) {
            OkHttpClient okHttpClient = new OkHttpClient();
            Request request = new Request.Builder().url(url).build();
            Response response = okHttpClient.newCall(request).execute();
            return response;
        }
        return null;
    }

    public static Response post(String url, Map<String, Object> params, Map<String, String> header) throws IOException {
        JSONObject jsonObject = JSONObject.parseObject(params.toString());
        return post(url, jsonObject.toJSONString(), header);
    }

    public static Response post(String url, JSONObject jsonObject, Map<String, String> headers) throws IOException {
        return post(url, jsonObject.toJSONString(), headers);
    }

    public static Response post(String url, String parameters, Map<String, String> headers) throws IOException {
        if (org.apache.commons.lang.StringUtils.isNotEmpty(url)) {
            OkHttpClient okHttpClient = new OkHttpClient();
            RequestBody requestBody = null;
            if (parameters != null) {
                requestBody = RequestBody.create(MediaType.parse("application/json"), parameters);
            } else {
                requestBody = RequestBody.create(MediaType.parse("application/json"), "");
            }
            Request.Builder builder = new Request.Builder().url(url).post(requestBody);
            if (headers != null) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    builder.addHeader(key, value);
                }
            }
            Request request = builder.build();
            Response response = okHttpClient.newCall(request).execute();
            return response;
        }
        return null;
    }

    public static Response postFile(String url, File file, Map<String, String> header) throws IOException {
        if (org.apache.commons.lang.StringUtils.isNotEmpty(url)) {
            String fileName = file.getName();
            OkHttpClient okHttpClient = new OkHttpClient();
            RequestBody fileBody = RequestBody.create(MediaType.parse("application/octet-stream"), file);
            MultipartBody multipartBody = new MultipartBody.Builder().setType(MultipartBody.FORM).addFormDataPart("template", fileName, fileBody).build();

            Request.Builder builder = new Request.Builder().url(url).post(multipartBody);
            if (header != null) {
                for (Map.Entry<String, String> entry : header.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    builder.addHeader(key, value);
                }
            }
            Request request = builder.build();
            Response response = okHttpClient.newCall(request).execute();
            return response;
        }
        return null;
    }

    public static Response put(String url, Map<String, String> params, Map<String, String> header) throws IOException {
        JSONObject jsonObject = JSONObject.parseObject(params.toString());
        return put(url, jsonObject.toJSONString(), header);
    }

    public static Response put(String url, JSONObject jsonObject, Map<String, String> header) throws IOException {
        return put(url, jsonObject.toJSONString(), header);
    }

    public static Response put(String url, String json, Map<String, String> header) throws IOException {
        if (org.apache.commons.lang.StringUtils.isNotEmpty(url)) {
            OkHttpClient okHttpClient = new OkHttpClient();
            RequestBody requestBody = null;
            if (json != null) {
                requestBody = RequestBody.create(MediaType.parse("application/json"), json);
            } else {
                requestBody = RequestBody.create(MediaType.parse("application/json"), "");
            }
            Request.Builder builder = new Request.Builder().url(url).put(requestBody);
            if (header != null) {
                for (Map.Entry<String, String> entry : header.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    builder.addHeader(key, value);
                }
            }
            Request request = builder.build();
            Response response = okHttpClient.newCall(request).execute();
            return response;
        }
        return null;
    }

}
