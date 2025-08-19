package com.stream;

import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.Properties;

/**
 * Flink 从 Kafka 读取数据并写入 Doris 的测试类
 * 功能：消费 Kafka 主题数据，打印到控制台，并同步写入 Doris 数据表
 */
public class FlinkSink2DorisTest {
    @SneakyThrows
    public static void main(String[] args) {
        // 1. 创建 Flink 流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 应用默认环境配置（如检查点、并行度、重启策略等，自定义工具类）
        EnvironmentSettingUtils.defaultParameter(env);

        // 3. 从 Kafka 读取数据，创建数据源
        // 参数说明：Kafka 集群地址、消费的主题名、消费者组 ID（使用当前时间戳避免冲突）、从最新偏移量开始消费
        DataStreamSource<String> dataStreamSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        "cdh01:9092,cdh02:9092,cdh03:9092",  // Kafka 集群地址
                        "zh_test",                             // 消费的 Kafka 主题
                        new Date().toString(),                 // 消费者组 ID（动态生成）
                        OffsetsInitializer.latest()            // 消费起始位置：最新偏移量
                ),
                WatermarkStrategy.noWatermarks(),  // 不使用水印（无事件时间处理需求）
                "test-kafka"                       // 数据源名称（用于 Flink UI 显示）
        );


        // 4. 调试：将从 Kafka 读取的数据打印到控制台
        dataStreamSource.print();

        // 5. 将数据写入 Doris 数据表
        // 调用自定义的 Doris 写入工具类，指定目标表名：dev_t_zh.dws_trade_cart_add_uu_window
        // 设置并行度为 1（根据实际需求调整）
        dataStreamSource.sinkTo(sink2DorisFunc("dev_t_zh.dws_trade_cart_add_uu_window"))
                .setParallelism(1);

        // 6. 启动 Flink 作业执行
        env.execute();
    }


    /**
     * 构建 Doris 写入器（Sink）的工具方法
     * @param tableName Doris 表名（格式：数据库名.表名）
     * @return 配置好的 DorisSink 实例
     */
    public static DorisSink<String> sink2DorisFunc(String tableName){
        // 1. 配置 Doris 数据格式相关参数
        Properties props = new Properties();
        props.setProperty("format", "json");               // 数据格式为 JSON
        props.setProperty("read_json_by_line", "true");     // 按行读取 JSON 数据

        // 2. 构建并返回 DorisSink 实例
        return DorisSink.<String>builder()
                // 设置 Doris 读取选项（默认配置）
                .setDorisReadOptions(DorisReadOptions.builder().build())
                // 配置 Doris 连接信息
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes("10.39.48.33:8030")  // Doris FE 节点地址（ Coordinator 节点）
                                .setTableIdentifier(tableName)   // 目标表标识（数据库.表名）
                                .setUsername("admin")            // Doris 用户名
                                .setPassword("zh1028,./")        // Doris 密码
                                .build()
                )
                // 配置 Doris 执行参数（Stream Load 相关）
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        // Stream Load 标签前缀（用于标识批次，避免重复导入）
                        .setLabelPrefix("doris_label_"+new Date().getTime())
                        .disable2PC() // 禁用两阶段提交（测试环境简化配置，生产环境需谨慎）
                        .setDeletable(false) // 不允许删除数据
                        .setBufferCount(4) // 缓冲区数据条数阈值（达到后触发写入）
                        .setBufferSize(1024*1024) // 缓冲区大小阈值（1MB，达到后触发写入）
                        .setMaxRetries(3) // 失败重试次数
                        .setStreamLoadProp(props) // 关联前面定义的 JSON 格式配置
                        .build())

                // 设置序列化器：将 String 类型数据直接序列化写入
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}
