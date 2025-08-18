package com.stream.common.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;
import java.util.List;

/**
 * TODO：用途：水印（Watermark）生成工具类。
 *  功能：
 *     事件时间水印计算（如 Flink 中的 AscendingTimestampExtractor）。
 *     处理乱序数据的时间对齐。
 *  典型场景：实时流处理中的时间窗口计算。
 **/
@Slf4j
public class WaterMarkUtils {

    public static WatermarkStrategy<JSONObject> getEthWarnWaterMark(long durationSeconds) {
        return WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(durationSeconds))
                .withTimestampAssigner((record, ts) -> {
                    long time;
                    time = record.containsKey("block_timestamp") ? record.getLong("block_timestamp") : record.getLong("timestamp");
                    return time * 1000;
                });
    }

    public static WatermarkStrategy<List<JSONObject>> getEthLiquidityWaterMark(long durationSeconds) {
        return WatermarkStrategy
                .<List<JSONObject>>forBoundedOutOfOrderness(Duration.ofSeconds(durationSeconds))
                .withTimestampAssigner((list, ts) -> {
                    JSONObject record = list.get(0);
                    return record.getLong("window_start_time");
                });
    }

    public static WatermarkStrategy<String> publicAssignWatermarkStrategy(String timestampField, long maxOutOfOrderlessSeconds) {
        return WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrderlessSeconds))
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(event);
                        if (event != null && jsonObject.containsKey(timestampField)) {
                            return jsonObject.getLong(timestampField);
                        }
                        return 0L;
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("Failed to parse event or get field '" + timestampField + "': " + event);
                        return 0L;
                    }
                });
    }

}
