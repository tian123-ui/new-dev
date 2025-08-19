package com.stream;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.FileUtils;
import com.stream.common.utils.KafkaUtils;

import java.util.Objects;

/**
 * 监听日志文件变化并将新内容同步到Kafka的工具类
 * 功能：检查目标日志文件是否有更新，若有则将最新内容发送到Kafka，并记录当前文件状态（时间戳）
 */
public class ListenLogFile2Kafka {
    // 从配置文件读取目标日志文件路径（需要监听的日志文件）
    private static final String REALTIME_LOG_FILE_PATH = ConfigUtils.getString("REALTIME.LOG.FILE.PATH");
    // 从配置文件读取日志位置记录文件路径（用于记录上次处理的文件时间戳）
    private static final String REALTIME_MSG_POSITION_FILE_PATH = ConfigUtils.getString("REALTIME.MSG.POSITION.FILE.PATH");
    // 从配置文件读取Kafka目标主题（日志内容要发送到的Kafka主题）
    private static final String REALTIME_KAFKA_LOG_TOPIC = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");


    /**
     * 主方法：程序入口，执行日志文件监听和同步逻辑
     */
    public static void main(String[] args) {

        // 核心逻辑：判断日志文件是否有更新
        // 1. 读取上次处理的文件时间戳（从位置记录文件中获取第一行数据）
        // 2. 获取当前日志文件的最后修改时间戳
        // 3. 若当前文件时间戳 > 上次记录的时间戳，说明文件有更新
        if (Long.parseLong(Objects.requireNonNull(FileUtils.getFileFirstLineData(REALTIME_MSG_POSITION_FILE_PATH)))
                <
                FileUtils.getFileLastTime(REALTIME_LOG_FILE_PATH)){
            // 日志文件有更新：读取文件内容并发送到Kafka
            KafkaUtils.sinkJson2KafkaMessage(REALTIME_KAFKA_LOG_TOPIC,FileUtils.readFileData(REALTIME_LOG_FILE_PATH));
            // 更新位置记录文件：将当前日志文件的最后修改时间戳写入，作为下次判断的基准
            FileUtils.sink2File(REALTIME_MSG_POSITION_FILE_PATH,String.valueOf(FileUtils.getFileLastTime(REALTIME_LOG_FILE_PATH)));
        }else {
            // 日志文件无更新：输出提示信息
            System.out.println("Message Log Is Last Data !");
        }

    }



}
