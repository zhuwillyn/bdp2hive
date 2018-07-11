package com.cecdata.bdp2hive.sqoop.vo;

/**
 * @author zhuweilin
 * @project bdp2hive
 * @description 对照关系实体类
 * @mail zhuwillyn@163.com
 * @date 2018/07/11 10:03
 */
public class RefInfo {

    private String orgName;
    private String orgCode;
    private String databaseName;
    private String tableName;
    private String fieldName;
    private String itemCode;
    private String structCode;
    private String databaseAddr;
    private String port;
    private String username;
    private String password;

    public RefInfo() {
    }

    public RefInfo(String orgName, String orgCode, String databaseName, String tableName, String fieldName, String itemCode, String structCode, String databaseAddr, String port, String username, String password) {
        this.orgName = orgName;
        this.orgCode = orgCode;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.itemCode = itemCode;
        this.structCode = structCode;
        this.databaseAddr = databaseAddr;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public String getDatabaseAddr() {
        return databaseAddr;
    }

    public void setDatabaseAddr(String databaseAddr) {
        this.databaseAddr = databaseAddr;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getOrgName() {
        return orgName;
    }

    public void setOrgName(String orgName) {
        this.orgName = orgName;
    }

    public String getOrgCode() {
        return orgCode;
    }

    public void setOrgCode(String orgCode) {
        this.orgCode = orgCode;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getItemCode() {
        return itemCode;
    }

    public void setItemCode(String itemCode) {
        this.itemCode = itemCode;
    }

    public String getStructCode() {
        return structCode;
    }

    public void setStructCode(String structCode) {
        this.structCode = structCode;
    }
}
