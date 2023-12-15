package com.kube.app.entity;

import java.io.Serializable;

public class CrawlingResult implements Serializable {

    public static final long serialVersionUID = 1L;

    private String schema;
    private long eventTime;
    private String s3Path;
    private String dataJson;

    public CrawlingResult() {
    }

    public CrawlingResult(String schema, long eventTime, String s3Path, String dataJson) {
        this.schema = schema;
        this.eventTime = eventTime;
        this.s3Path = s3Path;
        this.dataJson = dataJson;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getS3Path() {
        return s3Path;
    }

    public void setS3Path(String s3Path) {
        this.s3Path = s3Path;
    }

    public String getDataJson() {
        return dataJson;
    }

    public void setDataJson(String dataJson) {
        this.dataJson = dataJson;
    }
}
