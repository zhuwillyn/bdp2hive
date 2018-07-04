package com.cecdata.bdp2hive.common.vo;

/**
 * @author zhuweilin
 * @project transfer-tools
 * @description
 * @mail zhuwillyn@163.com
 * @date 2018/05/07 14:33
 */
public class DB {

    private int id;
    private int org;
    private String name;
    private String orgCode;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getOrg() {
        return org;
    }

    public void setOrg(int org) {
        this.org = org;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOrgCode() {
        return orgCode;
    }

    public void setOrgCode(String orgCode) {
        this.orgCode = orgCode;
    }
}
