package com.cecdata.bdp2hive.common.mapper;

import com.cecdata.bdp2hive.common.vo.DB;
import com.cecdata.bdp2hive.common.vo.DatasetCode;
import com.cecdata.bdp2hive.common.vo.Struct;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author zhuweilin
 * @project transfer-tools
 * @description
 * @mail zhuwillyn@163.com
 * @date 2018/05/07 13:53
 */
public interface Mapper {

    @Select("select id, dataset_struc_code from t_dataset_struc where dataset_struc_desc=#{name}")
    @Results({
            @Result(property = "id", column = "id"),
            @Result(property = "name", column = "dataset_struc_code")
    })
    Struct selectIdWithName(String name);

    @Select("select a.id, a.database_name, b.id as oid, b.medical_org_code from t_ds_database a,t_ds_org b where a.fk_belong_org = b.id and (a.conn_name=#{name} or a.database_name=#{name})")
    @Results({
            @Result(property = "id", column = "id"),
            @Result(property = "name", column = "database_name"),
            @Result(property = "org", column = "oid"),
            @Result(property = "orgCode", column = "medical_org_code")
    })
    DB selectWithName(String name);

    @Select("select a.dataset_struc_code,b.dataset_item_code from t_dataset_struc a, t_dataset_item b where a.id=b.fk_dataset_struc_id and a.dataset_struc_level=4")
    @Results({
            @Result(property = "structCode", column = "dataset_struc_code"),
            @Result(property = "itemCode", column = "dataset_item_code")
    })
    List<DatasetCode> selectDatasetAndItemCode();

}
