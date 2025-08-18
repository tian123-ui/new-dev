package com.stream.common.utils;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * TODO:用途：自定义字符串序列化/反序列化方案。
 *  功能：
 *  实现特定格式的数据序列化（如 JSON、Protobuf）。
 *  在流处理框架（如 Flink/Kafka）中使用。
 *  典型场景：数据管道中的数据格式转换。
 */
public class CustomStringSerializationSchema implements KafkaSerializationSchema<String> {
    private final String topic;

    public CustomStringSerializationSchema(String topic) {
        this.topic = topic;
    }
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
        return new ProducerRecord<>(topic, null, s.getBytes(StandardCharsets.UTF_8));
    }
}
