package com.stream.common.utils;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;

import com.stream.common.domain.HdfsInfo;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * TODO:Flink流处理中与HDFS交互的工具类
 *  主要功能：
 *  1. 构建HDFS配置信息对象(HdfsInfo)
 *  2. 根据配置生成Flink的StreamingFileSink，支持按天分区或直接写入
 *  注意：输入数据需为JSON格式字符串（当启用分区时）
 */
public class HdfsUtils {

    /**
     * 构建HDFS配置信息对象
     * @param url HDFS基础路径（例如：hdfs:///user/flink/output）
     * @param hdfsNeedPartition 是否需要按分区写入
     * @param hdfsPartitionField 分区依赖的时间戳字段名（仅当需要分区时有效）
     * @return 封装好的HdfsInfo对象
     */
    public static HdfsInfo buildHdfsInfo(String url, Boolean hdfsNeedPartition, String hdfsPartitionField) {
        // 初始化HDFS配置对象
        HdfsInfo hdfsInfo = new HdfsInfo();
        hdfsInfo.setHdfsUrl(url);
        hdfsInfo.setHdfsNeedPartition(hdfsNeedPartition);
        hdfsInfo.setHdfsPartitionField(hdfsPartitionField);
        return hdfsInfo;
    }

    /**
     * 根据HDFS配置生成Flink的StreamingFileSink
     * 用于将流数据写入HDFS（或其他文件系统），支持分区和滚动策略配置
     * @param hdfsInfo HDFS配置信息对象
     * @return 配置好的StreamingFileSink实例
     */
    public static StreamingFileSink<String> getCommonSink(HdfsInfo hdfsInfo) {
        BucketAssigner<String, String> bucketAssigner;

        if (hdfsInfo.isHdfsNeedPartition()) {
            // 定义桶分配器（决定数据写入哪个子目录/分区）
            bucketAssigner = new BucketAssigner<String, String>() {
                /**
                 * 计算当前数据应写入的分区目录
                 * @param input 输入的JSON格式字符串数据
                 * @param context 桶分配器上下文（包含时间等信息）
                 * @return 分区目录名（格式：day=yyyyMMdd 或 error_bucket）
                 */
                @Override
                public String getBucketId(String input, Context context) {
                    // 默认错误分区目录
                    String bucketId = "error_bucket";
                    try {
                        // 解析JSON获取时间戳字段值
                        // 注意：输入数据必须为JSON格式，且包含指定的时间戳字段
                        long timestamp = JSON.parseObject(input).getLongValue(hdfsInfo.getHdfsPartitionField());
                        // 时间戳单位转换：若为秒级（<10位）则转为毫秒级
                        // 10000000000L 约等于2001年9月9日（用于区分秒级/毫秒级时间戳）
                        if (timestamp < 10000000000L) {
                            timestamp = timestamp * 1000;
                        }
                        // 生成按天分区的目录名（例如：day=20240821）
                        bucketId = "day=" + DateUtil.format(new Date(timestamp), "yyyyMMdd");
                        // 调试用：打印生成的分区目录（生产环境建议替换为日志框架）
                        System.err.println(bucketId);
                    } catch (Exception e) {
                        // 异常处理：解析失败时写入error_bucket
                        e.printStackTrace();
                    }
                    return bucketId;
                }

                /**
                 * 获取桶ID的序列化器
                 * @return 字符串序列化器实例
                 */
                @Override
                public SimpleVersionedSerializer<String> getSerializer() {
                    return SimpleVersionedStringSerializer.INSTANCE;
                }
            };
        } else {
            // 不启用分区：使用Flink默认的基础路径分配器（所有数据直接写入根目录）
            bucketAssigner = new BasePathBucketAssigner<>();
        }

        // 构建并返回StreamingFileSink
        return StreamingFileSink
                // 配置行格式输出：指定输出路径和字符串编码器（UTF-8编码）
                .forRowFormat(new Path(hdfsInfo.getHdfsUrl()), new SimpleStringEncoder<String>("UTF-8"))
                // 设置桶分配器（决定分区逻辑）
                .withBucketAssigner(bucketAssigner)
                // 配置文件滚动策略（决定何时生成新文件）
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        // 滚动间隔：每10分钟生成一个新文件
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
                        // 无活动间隔：5分钟无数据写入则生成新文件
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        // 最大文件大小：达到128MB时生成新文件
                        .withMaxPartSize(1024 * 1024 * 128)
                        .build())
                .build();
    }
}
