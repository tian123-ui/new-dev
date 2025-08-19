package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDimFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;




public class DbusCdc2DimHbaseAnd2DbKafka {


    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String MYSQL_CDC_TO_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root"); // 设置 Hadoop 用户名（用于HDFS/HBase权限控制）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();// 获取 Flink 执行环境
        EnvironmentSettingUtils.defaultParameter(env);// 应用默认环境配置（如检查点、重启策略等）

        // 创建主业务数据库的 CDC 数据源
        // 监听整个数据库的所有表变更
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),// 数据库名
                "", // 空表名表示监听所有表
                ConfigUtils.getString("mysql.user"), // 数据库用户名
                ConfigUtils.getString("mysql.pwd"),// 数据库密码
                StartupOptions.initial()  // 从初始位置开始读取binlog
        );

        // 创建维度配置数据库的 CDC 数据源
        // 仅监听特定的维度表配置表（realtime_v1_config.table_process_dim）
        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),           // 配置库名
                "realtime_v1_config.table_process_dim",                  // 特定配置表
                ConfigUtils.getString("mysql.user"),                     // 数据库用户名
                ConfigUtils.getString("mysql.pwd"),                      // 数据库密码
                StartupOptions.initial()                                 // 从初始位置开始
        );

        // 从 CDC 源创建数据流 - 主业务数据流
        DataStreamSource<String> cdcDbMainStream = env.fromSource(
                mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), // 无水印策略（CDC场景常用）
                "mysql_cdc_main_source" // 数据源名称（用于监控）
        );
        // 从 CDC 源创建数据流 - 维度配置数据流
        DataStreamSource<String> cdcDbDimStream = env.fromSource(
                mySQLCdcDimConfSource,
                WatermarkStrategy.noWatermarks(),
                "mysql_cdc_dim_source"
        );
        // 主业务数据流处理：字符串 → JSONObject
        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream.map(JSONObject::parseObject)// 将JSON字符串解析为JSONObject
                .uid("db_data_convert_json") // 唯一标识符（用于故障恢复）
                .name("db_data_convert_json")// 任务名称（显示在Flink Web UI）
                .setParallelism(1);// 并行度设为1，保证顺序性

        // 将主业务数据写入Kafka
        cdcDbMainStreamMap.map(JSONObject::toString)// 转回字符串格式
        .sinkTo(
                        KafkaUtils.buildKafkaSink(CDH_KAFKA_SERVER, MYSQL_CDC_TO_KAFKA_TOPIC)
                )
                .uid("mysql_cdc_to_kafka_topic")
                .name("mysql_cdc_to_kafka_topic");

        // 调试输出：打印主业务数据流
        cdcDbMainStreamMap.print("cdcDbMainStreamMap -> ");

        // 维度配置数据流处理：字符串 → JSONObject
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                .setParallelism(1);

        // 清洗维度配置数据：移除冗余字段，标准化JSON结构
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn = cdcDbDimStreamMap.map(s -> {
            // 移除CDC框架自带的元数据字段
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    // 根据操作类型选择before或after数据
                    if ("d".equals(s.getString("op"))){
                        resJson.put("before",s.getJSONObject("before"));
                    }else {
                        resJson.put("after",s.getJSONObject("after"));
                    }
                    resJson.put("op",s.getString("op"));
                    return resJson;
                }).uid("clean_json_column_map")
                .name("clean_json_column_map");

       // 处理维度表变更：根据配置更新HBase维度表
       // 调用自定义函数MapUpdateHbaseDimTableFunc进行HBase表结构管理
        SingleOutputStreamOperator<JSONObject> tpDS = cdcDbDimStreamMapCleanColumn.map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER, CDH_HBASE_NAME_SPACE))
                .uid("map_create_hbase_dim_table")
                .name("map_create_hbase_dim_table");


        // 定义广播状态描述符
        MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
        BroadcastStream<JSONObject> broadcastDs = tpDS.broadcast(mapStageDesc);   // 将维度数据流广播到所有并行实例
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcDbMainStreamMap.connect(broadcastDs);// 连接主业务数据流和广播的维度流

        // 使用ProcessFunction处理连接流，实现维度关联
        connectDs.process(new ProcessSpiltStreamToHBaseDimFunc(mapStageDesc));


        // 禁用算子链（便于监控和调试）
        env.disableOperatorChaining();
        // 启动Flink任务执行
        env.execute();
    }

}
