package com.stream.common;

import com.stream.common.utils.ConfigUtils;

/**
 * 本类演示了如何通过 ConfigUtils 工具类从配置文件中读取参数值，
 * 是配置管理系统的基础使用范例。
 * 从配置中心获取 Kafka 错误日志路径参数
 *
 * TODO：CommonTest 类解析与中文注释
 */
public class CommonTest {

    public static void main(String[] args) {

        // 从配置系统获取 Kafka 错误日志存储路径
        // 配置键说明：kafka.err.log = Kafka 组件错误日志的文件路径
        String kafka_err_log = ConfigUtils.getString("kafka.err.log");

        // 将配置值输出到标准错误流（便于与普通日志区分）
        // 注意：生产环境应使用日志框架（如 SLF4J）而非直接打印
        System.err.println(kafka_err_log);

    }
}
