package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDimFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;




/**
 * TODO： 实时同步数据dim
 * @Package com.retailersv1.DbusCdc2DimHbaseAnd2DbKafka

 *  本程序实现以下两个核心功能：
 *  1. 将 MySQL 主业务库的 CDC（变更数据捕获）数据实时同步到 Kafka 的指定主题（realtime_db），用于后续实时数仓处理。
 *  2. 监听配置库中的维度表处理规则表（table_process_dim），动态更新 HBase 中的维度表结构或元数据，并广播该配置，
 *     然后结合主业务流和维度配置流，将主数据按照配置写入 HBase 的对应维度表中。
 *
 *  整体流程如下：
 *  - 使用 Flink CDC 从两个 MySQL 数据库读取 binlog 变更日志：
 *      a) 主业务数据库（db_main） -> 所有表变更事件
 *      b) 配置数据库（db_conf）中的表 `realtime_v1_config.table_process_dim` -> 维度同步规则变更
 *  - 主业务流：JSON 化后既输出到 Kafka，也用于后续与维度配置进行连接处理。
 *  - 维度配置流：清洗后触发 HBase 表的创建/更新操作，并作为广播状态供主数据流使用。
 *  - 使用 Broadcast State 模式，将最新的维度同步规则广播给每个算子实例，主数据流根据规则决定如何写入 HBase。
 */
public class DbusCdc2DimHbaseAnd2DbKafka {

    // 从配置文件中加载的关键环境参数
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String MYSQL_CDC_TO_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    /**
     * 主入口方法
     *
     * @param args 命令行参数（未使用）
     * @throws Exception 异常由 @SneakyThrows 自动抛出
     */
    @SneakyThrows
    public static void main(String[] args) {

        // 设置 Hadoop 用户名为 root，用于访问 HDFS/HBase 等安全集群资源
        System.setProperty("HADOOP_USER_NAME","root");
        // 获取 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 应用默认的执行环境设置（如并行度、检查点、重启策略等）
        EnvironmentSettingUtils.defaultParameter(env);

       // ==================== 步骤一：构建主业务库的 CDC 源 ====================
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"), // 要监听的主数据库名
                "", // 空白表示监听该库下所有表
                ConfigUtils.getString("mysql.user"),  // 数据库用户名
                ConfigUtils.getString("mysql.pwd"),  // 数据库密码
                StartupOptions.initial() // 启动模式：从初始快照开始读取
        );

        // ==================== 步骤二：构建维度配置库的 CDC 源 ====================
        // 读取配置库的变化binlog
        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),// 配置库名称（如 realtime_v1_config）
                "realtime_v1_config.table_process_dim", // 明确监听的配置表
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial() // 同样从初始状态开始同步
        );

        // ==================== 步骤三：创建数据流 ====================
        // 从主业务库源创建数据流
        DataStreamSource<String> cdcDbMainStream = env.fromSource(
                mySQLDbMainCdcSource,
                WatermarkStrategy.noWatermarks(),// CDC 场景通常不需要水印
                "mysql_cdc_main_source"
        );

        // 从维度配置库源创建数据流
        DataStreamSource<String> cdcDbDimStream = env.fromSource(
                mySQLCdcDimConfSource,
                WatermarkStrategy.noWatermarks(),
                "mysql_cdc_dim_source"
        );

        // ==================== 步骤四：主业务流处理 ====================

        // 将主业务流的字符串 JSON 转换为 JSONObject 对象
        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream
                .map(JSONObject::parseObject) // 配置库名称（如 realtime_v1_config）
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1); // 设置并行度为1，保证顺序性（可选）

        // 将主业务数据原样发送到 Kafka 的指定主题，供下游系统消费
        cdcDbMainStreamMap.map(JSONObject::toString) // 转回字符串
        .sinkTo(
                        KafkaUtils.buildKafkaSink(CDH_KAFKA_SERVER, MYSQL_CDC_TO_KAFKA_TOPIC)
                )
                .uid("mysql_cdc_to_kafka_topic")
                .name("mysql_cdc_to_kafka_topic");

        // 调试用：打印主业务流内容到控制台
        cdcDbMainStreamMap.print("cdcDbMainStreamMap -> ");


        // ==================== 步骤五：维度配置流处理 ====================
        // 将配置流字符串转为 JSONObject
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                .setParallelism(1);

        // 清洗配置流数据：移除无用字段，标准化结构
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn = cdcDbDimStreamMap.map(s -> {
                    s.remove("source"); // 删除 source 字段（包含数据库、位点等元信息）
                    s.remove("transaction"); // 删除 transaction 字段（事务相关信息）


                    JSONObject resJson = new JSONObject();
                    // 根据操作类型 "op"（c=insert, u=update, d=delete）提取 before 或 after 数据
                    if ("d".equals(s.getString("op"))){
                        resJson.put("before",s.getJSONObject("before"));
                    }else {
                        resJson.put("after",s.getJSONObject("after"));
                    }
                    resJson.put("op",s.getString("op")); // 保留操作类型
                    return resJson;
                }).uid("clean_json_column_map")
                .name("clean_json_column_map");


        // 调用自定义 MapFunction，根据配置表内容在 HBase 中创建或更新维度表
        // 输入是清洗后的配置变更事件，输出可能是成功标志或处理后的配置对象
        SingleOutputStreamOperator<JSONObject> tpDS = cdcDbDimStreamMapCleanColumn.map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER, CDH_HBASE_NAME_SPACE))
                .uid("map_create_hbase_dim_table")
                .name("map_create_hbase_dim_table");


         // ==================== 步骤六：广播维度配置并连接主数据流 ====================
        // 定义广播状态描述符：key 是表名或其他标识，value 是对应的同步配置（JSON）
        MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>(
                "mapStageDesc",
                String.class,
                JSONObject.class
        );

        // 将维度配置流（tpDS）广播出去，所有并行任务都能接收到最新配置
        BroadcastStream<JSONObject> broadcastDs = tpDS.broadcast(mapStageDesc);
        // 将主业务流与广播的维度配置流连接起来，形成一个可处理的连接流
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcDbMainStreamMap.connect(broadcastDs);

        // 使用自定义 ProcessFunction 处理连接流：
        // - 接收主数据（非广播流）
        // - 接收广播的维度配置更新
        // - 根据当前配置决定是否将主数据写入 HBase 的某个维度表
        connectDs.process(new ProcessSpiltStreamToHBaseDimFunc(mapStageDesc));


        // ==================== 步骤七：最终执行 ====================
        // 禁用算子链（operator chaining），便于调试和监控每个独立算子
        env.disableOperatorChaining();
        // 启动 Flink 作业
        env.execute();
    }

}
