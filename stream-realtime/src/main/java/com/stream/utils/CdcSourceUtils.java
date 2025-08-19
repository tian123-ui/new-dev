package com.stream.utils;

import com.stream.common.utils.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import java.util.Properties;

/**
 * TODO：Mysql CDC数据源工具类
 *
 */
public class CdcSourceUtils {

    public static MySqlSource<String> getMySQLCdcSource(
            String database,
            String table,
            String username,
            String pwd,StartupOptions model){

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.connectionCharset", "UTF-8");
        debeziumProperties.setProperty("decimal.handling.mode","string");
        debeziumProperties.setProperty("time.precision.mode","connect");
        debeziumProperties.setProperty("snapshot.mode", "schema_only");
        debeziumProperties.setProperty("include.schema.changes", "false");
        debeziumProperties.setProperty("database.connectionTimeZone", "Asia/Shanghai");

        // 构建 MySQL CDC 数据源
        return MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))  // 从配置中心获取主机地址
                .port(ConfigUtils.getInt("mysql.port"))         // 获取端口号
                .databaseList(database)                         // 指定要监听的数据库
                .tableList(table)                               // 指定要监听的表（空字符串表示所有表）
                .username(username)                             // 数据库用户名
                .password(pwd)                                  // 数据库密码
                .serverId("5403-5450")                          // 指定serverId范围，避免冲突
                // .connectionTimeZone(ConfigUtils.getString("mysql.timezone")) // 时区配置（已注释）
                .deserializer(new JsonDebeziumDeserializationSchema())  // JSON反序列化器
                .startupOptions(model)                          // 启动模式（由调用方指定）
                .includeSchemaChanges(true)                     // 包含模式变更事件
                .debeziumProperties(debeziumProperties)         // 底层Debezium配置
                .build();
    }
}
