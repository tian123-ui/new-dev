package com.stream;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.ververica.connectors.common.sink.OutputFormatSinkFunction;
import com.alibaba.ververica.connectors.hologres.api.HologresRecordConverter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.config.HologresConfigs;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCWriter;
import com.alibaba.ververica.connectors.hologres.sink.HologresOutputFormat;
import com.stream.common.utils.ConfigUtils;
import com.stream.domain.MySQLMessageInfo;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

/**
 * Flink CDC 从 MySQL 监听 binlog 变更数据，转换后写入 Hologres 的程序
 * 功能：通过 Debezium 捕获 MySQL 表的增删改操作，解析为结构化数据后写入 Hologres 表
 */
public class DbusMySQLCdc2Holo {

    public static void main(String[] args) throws Exception {
        // 1. 构建 MySQL CDC 数据源（监听 binlog 变更）
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host")) // MySQL 主机地址（从配置文件读取）
                .port(ConfigUtils.getInt("mysql.port")) // MySQL 端口（从配置文件读取）
                .databaseList(ConfigUtils.getString("mysql.database")) // 需要监听的数据库
                .tableList("business_dev.test.cdc") // 需要监听的表（格式：库名.表名）
                .username(ConfigUtils.getString("mysql.user")) // MySQL 用户名
                .password(ConfigUtils.getString("mysql.pwd")) // MySQL 密码
                .serverTimeZone(ConfigUtils.getString("mysql.timezone")) // 时区配置（如 Asia/Shanghai）
                .deserializer(new JsonDebeziumDeserializationSchema()) // 反序列化器：将 binlog 事件转为 JSON 字符串
                .startupOptions(StartupOptions.latest()) // 启动策略：从最新的 binlog 位置开始监听
                .includeSchemaChanges(true) // 是否包含表结构变更事件
                .build();

        // 2. 创建 Flink 流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 3. 从 MySQL CDC 源获取数据流
        DataStreamSource<String> dataStreamSource = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(), // 无需水印（无事件时间处理需求）
                "mysql-source-cdc-listen" // 数据源名称（用于 Flink UI 标识）
        );
        // 调试：打印原始 CDC 数据（JSON 字符串格式）
        dataStreamSource.print();

        // 4. 数据转换：JSON 字符串 → JSONObject
        SingleOutputStreamOperator<JSONObject> map = dataStreamSource.map(JSONObject::parseObject);

        // 5. 数据解析：JSONObject → 自定义实体类 MySQLMessageInfo
        // 提取 CDC 事件中的关键字段（如操作类型、数据库名、表名、变更前后数据等）
        SingleOutputStreamOperator<MySQLMessageInfo> res = map.map(json -> {
            MySQLMessageInfo mySQLMessageInfo = new MySQLMessageInfo();
            mySQLMessageInfo.setId(json.getString("id")); // 数据主键
            mySQLMessageInfo.setOp(json.getString("op")); // 操作类型（c-新增、u-更新、d-删除）
            mySQLMessageInfo.setDb_name(json.getString("database")); // 数据库名
            mySQLMessageInfo.setLog_before(json.getString("before")); // 变更前数据（JSON 字符串）
            mySQLMessageInfo.setLog_after(json.getString("after")); // 变更后数据（JSON 字符串）
            mySQLMessageInfo.setT_name(json.getString("tableName")); // 表名
            mySQLMessageInfo.setTs(json.getString("ts")); // 事件时间戳
            return mySQLMessageInfo;
        });

        // 6. 定义 Hologres 目标表结构（与实体类字段对应）
        TableSchema tableSchema = TableSchema.builder()
                .field("id", DataTypes.STRING()) // 对应实体类的 id 字段
                .field("op", DataTypes.STRING()) // 对应实体类的 op 字段
                .field("db_name", DataTypes.STRING()) // 对应实体类的 db_name 字段
                .field("log_before", DataTypes.STRING()) // 对应实体类的 log_before 字段
                .field("log_after", DataTypes.STRING()) // 对应实体类的 log_after 字段
                .field("t_name", DataTypes.STRING()) // 对应实体类的 t_name 字段
                .field("ts", DataTypes.STRING()) // 对应实体类的 ts 字段
                .build();

        // 7. 配置 Hologres 连接参数
        HologresConnectionParam hologresConnectionParam = hologresConfig();

        // 调试：打印转换后的实体类数据
        res.print();

        // 8. 将数据写入 Hologres
        res.addSink(
                new OutputFormatSinkFunction<MySQLMessageInfo>( // Flink Sink 包装器
                        new HologresOutputFormat<>( // Hologres 输出格式
                                hologresConnectionParam, // 连接参数
                                new HologresJDBCWriter<>( // JDBC 写入器
                                        hologresConnectionParam,
                                        tableSchema, // 目标表结构
                                        new RecordConverter(hologresConnectionParam) // 自定义数据转换器
                                ))
                )
        );

        // 启动 Flink 作业（当前注释掉，实际运行需开启）
//        env.disableOperatorChaining(); // 禁用算子链（便于调试时查看各步骤）
//        env.execute();
    }

    /**
     * 配置 Hologres 连接参数
     * @return Hologres 连接参数对象
     */
    private static HologresConnectionParam hologresConfig(){
        Configuration configuration = new Configuration();
        configuration.set(HologresConfigs.ENDPOINT, ConfigUtils.getString("holo.endpoint")); // Hologres 连接地址（host:port）
        configuration.set(HologresConfigs.DATABASE, ConfigUtils.getString("holo.database")); // 目标数据库名
        configuration.set(HologresConfigs.USERNAME, ConfigUtils.getString("ali.key")); // 访问密钥（AK）
        configuration.set(HologresConfigs.PASSWORD, ConfigUtils.getString("ali.pwd")); // 访问密钥（SK）
        configuration.set(HologresConfigs.TABLE, "public.mysql_binlog_info"); // 目标表名（格式：schema.表名）
        configuration.set(HologresConfigs.MUTATE_TYPE, "insertorupdate"); // 写入策略：存在则更新，不存在则插入
        configuration.set(HologresConfigs.OPTIONAL_SINK_IGNORE_DELETE, false); // 不忽略删除操作（删除事件也会写入）
        configuration.setBoolean(HologresConfigs.CREATE_MISSING_PARTITION_TABLE, true); // 自动创建缺失的分区表

        return new HologresConnectionParam(configuration);
    }

    /**
     * 自定义数据转换器：将 MySQLMessageInfo 实体类转换为 Hologres 可识别的 Record 对象
     */
    public static class RecordConverter implements HologresRecordConverter<MySQLMessageInfo, Record> {

        private final HologresConnectionParam hologresConnectionParam;
        private HologresTableSchema tableSchema; // Hologres 表结构（延迟初始化，从连接参数中获取）

        public RecordConverter(HologresConnectionParam hologresConnectionParam) {
            this.hologresConnectionParam = hologresConnectionParam;
        }

        /**
         * 转换方法：将 MySQLMessageInfo 转换为 Hologres Record
         * @param message 从 CDC 解析的实体类数据
         * @return Hologres 记录对象（可直接写入）
         */
        @Override
        public Record convertFrom(MySQLMessageInfo message) {
            // 延迟初始化 Hologres 表结构（从连接参数中获取目标表的元数据）
            if (tableSchema == null) {
                this.tableSchema = HologresTableSchema.get(hologresConnectionParam.getJdbcOptions());
            }

            // 调试：打印表结构和待转换的数据
            System.err.println("------data start------");
            System.err.println(tableSchema.toString());
            System.err.println(message);
            System.err.println("------data end------");

            // 创建 Hologres 记录对象（与表结构字段顺序对应）
            Record result = new Record(tableSchema.get());
            result.setObject(0, message.getId()); // 第1个字段：id
            result.setObject(1, message.getOp()); // 第2个字段：op
            result.setObject(2, message.getDb_name()); // 第3个字段：db_name
            result.setObject(3, message.getLog_before()); // 第4个字段：log_before
            result.setObject(4, message.getLog_after()); // 第5个字段：log_after
            result.setObject(5, message.getT_name()); // 第6个字段：t_name
            result.setObject(6, message.getTs()); // 第7个字段：ts

            // 调试：打印转换后的 Record
            System.err.println("result -> "+result);

            return result;
        }

        /**
         * 反向转换（从 Record 到实体类）：本场景无需实现
         */
        @Override
        public MySQLMessageInfo convertTo(Record record) {
            throw new UnsupportedOperationException("No need to implement");
        }

        /**
         * 转换为主键记录：本场景无需实现
         */
        @Override
        public Record convertToPrimaryKey(MySQLMessageInfo jsonObject) {
            throw new UnsupportedOperationException("No need to implement");
        }
    }
}