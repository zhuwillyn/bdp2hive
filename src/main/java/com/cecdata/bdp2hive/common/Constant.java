package com.cecdata.bdp2hive.common;

/**
 * @author zhuweilin
 * @project transfer-tools
 * @description
 * @mail zhuwillyn@163.com
 * @date 2018/05/07 11:47
 */
public class Constant {

    public static class SQOOP {
        public static final String REF_URL = "ref_url";
        public static final String SQL_URL = "sql_url";
        public static final String SQL_PARAM = "sql_param";

        public static final String SCRIPT = "script";
        public static final String SCRIPT_PARTITION = "partition_script";

        public static final String SQOOP_REF_SQL = "sqoop.ref.sql";
        public static final String SQOOP_REF_SQL_CONDITION = "sqoop.ref.sql.condition";
        public static final String SQOOP_ITEM_SQL = "sqoop.item.sql";

        public static final String COLUMN_ORG_NAME = "medical_org_name";
        public static final String COLUMN_ORG_CODE = "medical_org_code";
        public static final String COLUMN_DATABASE_NAME = "database_name";
        public static final String COLUMN_TABLE_NAME = "table_name";
        public static final String COLUMN_FIELD_NAME = "field_name";
        public static final String COLUMN_ITEM_CODE = "dataset_item_code";
        public static final String COLUMN_STRUCT_CODE = "dataset_struc_code";
        public static final String COLUMN_STRUCT_ITEM_CODE = "dataset_item_code";
        public static final String COLUMN_DATABASE_ADDRESS = "database_address";
        public static final String COLUMN_ACCESS_PORT = "access_port";
        public static final String COLUMN_USERNAME = "user_name";
        public static final String COLUMN_PASSWORD = "password";

        public static final String DATASET_YSJKMC = "YSJKMC";
        public static final String DATASET_YSJBMC = "YSJBMC";
        public static final String DATASET_SJYYLJGMC = "SJYYLJGMC";
        public static final String DATASET_SJYYLJGDM = "SJYYLJGDM";

    }

    public static class HIVE {

    }

    public static class Cli {

        public static class Sqoop {
            public static final String HELP_SHORT = "h";
            public static final String HELP_LONG = "help";
            public static final String SQOOP_BIN_PATH_SHORT = "s";
            public static final String SQOOP_BIN_PATH_LONG = "sqoop_cmd_path";
            public static final String MYSQL_URL_SHORT = "u";
            public static final String MYSQL_URL_LONG = "mysql_url";
            public static final String MYSQL_USER_SHORT = "n";
            public static final String MYSQL_USER_LONG = "mysql_user";
            public static final String MYSQL_PASSWORD_SHORT = "w";
            public static final String MYSQL_PASSWORD_LONG = "mysql_password";
            public static final String ORG_CODE_SHORT = "o";
            public static final String ORG_CODE_LONG = "org_code";
            public static final String HIVE_DATABASE_SHORT = "d";
            public static final String HIVE_DATABASE_LONG = "hive_database";
        }

        public static class Hive {
            public static final String HELP_SHORT = "h";
            public static final String HELP_LONG = "help";
            public static final String HIVE_URL_SHORT = "U";
            public static final String HIVE_URL_LONG = "hive-url";
            public static final String HIVE_USER_SHORT = "N";
            public static final String HIVE_USER_LONG = "hive-user";
            public static final String HIVE_PASSWORD_SHORT = "W";
            public static final String HIVE_PASSWORD_LONG = "hive-password";
            public static final String MYSQL_URL_SHORT = "u";
            public static final String MYSQL_URL_LONG = "mysql-url";
            public static final String MYSQL_USER_SHORT = "n";
            public static final String MYSQL_USER_LONG = "mysql-user";
            public static final String MYSQL_PASSWORD_SHORT = "w";
            public static final String MYSQL_PASSWORD_LONG = "mysql-password";
        }

    }



}
