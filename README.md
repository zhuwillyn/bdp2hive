### bdp2hive项目介绍
---
#### 一、概述
项目主要是集成生成sqoop脚本和创建hive分区表组件。生成sqoop脚本组件主要通过调取治理平台接口获取表字段与数据集之间的对照关系的SQL语句，通过模板拼接成sqoop脚本，上传服务器执行；hive分区表生成组件主要是通过获取数据集及数据集item的code，数据集code作为hive表名，数据集item code作为hive分区表字段，同时加入相关字段，形成hive表的基本结构。

##### 项目结构

```
─src
   ├─main
   │  ├─java
   │  │  └─com
   │  │      └─cecdata
   │  │          └─bdp2hive
   │  │              ├─common
   │  │              │  ├─mapper
   │  │              │  └─vo
   │  │              ├─hive
   │  │              └─sqoop
   │  │                  ├─service
   │  │                  └─util
   │  └─resources
   └─test
       └─java
```

#### 二、打包
通过maven组件整体打成jar包，如下：
```
# mvn命令行打包
mvn clean package
# 若要跳过测试
mvn clean package -Dmaven.test.skip=true
```
也可以通过idea插件打包，这里不予赘述。

#### 三、运行方式
因为是jar包，所以运行环境必须安装jdk1.8+，具体运行方式：
```
java -jar bdp2hive-1.0.jar [hive|sqoop] <args>
```
其中[hive|sqoop]表示为子命令，必选其一，hive表示是生成hive分区表组件，sqoop表示是生成sqoop组件，下面介绍具体的参数：
##### 1、hive子命令
```
usage: java -jar bdp2hive-1.0.jar hive [-h] [-N <arg>] [-n <arg>] [-U <arg>] [-u <arg>] [-W] [-w <arg>]
 -h,--help                   Print help
 -N,--hive_user <arg>        The username of Hive #hive数据库用户名
 -n,--mysql_user <arg>       The username of MySQL #mysql数据库用户名
 -U,--hive_url <arg>         The url of Hive connect    #hive数据库的jdbc连接（不指定库默认为default库）
 -u,--mysql_url <arg>        The url of MySQL   #mysql数据库的jdbc连接
 -W,--hive_password          The password of Hive   #hive数据库用户名对应的密码（若未开启hive认证可为空）
 -w,--mysql_password <arg>   The password of MySQL  #mysql数据库用户名对应的密码
```

##### 2、sqoop子命令
相比于hive子命令，sqoop子命令输入的参数相对较少的多：
```
usage: java -jar bdp2hive-1.0.jar sqoop [-f <arg>] [-h]
 -f,--file_path <arg>   The path of the excel file  #刘博那边处理好的数据库表与数据集的对照关系excel文件（可指定目录，意为目录下的所有excel文件）
 -h,--help              Print help  #打印帮助

```