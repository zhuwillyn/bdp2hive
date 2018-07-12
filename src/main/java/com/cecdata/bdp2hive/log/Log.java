package com.cecdata.bdp2hive.log;

import java.util.Date;

/**
 * @author zhuweilin
 * @project bdp2hive
 * @description
 * @mail zhuwillyn@163.com
 * @date 2018/07/12 14:44
 */
public class Log {

    private String srcOrgName;
    private String srcOrgCode;
    private String srcUrl;
    private String srcDatabase;
    private String srcTable;
    private Long srcCount;
    private String dstUrl;
    private String dstDatabase;
    private String dstTable;
    private Long dstCount;
    private Integer status;
    private Date createTime;

    public String getSrcOrgName() {
        return srcOrgName;
    }

    public void setSrcOrgName(String srcOrgName) {
        this.srcOrgName = srcOrgName;
    }

    public String getSrcOrgCode() {
        return srcOrgCode;
    }

    public void setSrcOrgCode(String srcOrgCode) {
        this.srcOrgCode = srcOrgCode;
    }

    public String getSrcUrl() {
        return srcUrl;
    }

    public void setSrcUrl(String srcUrl) {
        this.srcUrl = srcUrl;
    }

    public String getSrcDatabase() {
        return srcDatabase;
    }

    public void setSrcDatabase(String srcDatabase) {
        this.srcDatabase = srcDatabase;
    }

    public String getSrcTable() {
        return srcTable;
    }

    public void setSrcTable(String srcTable) {
        this.srcTable = srcTable;
    }

    public Long getSrcCount() {
        return srcCount;
    }

    public void setSrcCount(Long srcCount) {
        this.srcCount = srcCount;
    }

    public String getDstUrl() {
        return dstUrl;
    }

    public void setDstUrl(String dstUrl) {
        this.dstUrl = dstUrl;
    }

    public String getDstDatabase() {
        return dstDatabase;
    }

    public void setDstDatabase(String dstDatabase) {
        this.dstDatabase = dstDatabase;
    }

    public String getDstTable() {
        return dstTable;
    }

    public void setDstTable(String dstTable) {
        this.dstTable = dstTable;
    }

    public Long getDstCount() {
        return dstCount;
    }

    public void setDstCount(Long dstCount) {
        this.dstCount = dstCount;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
}
