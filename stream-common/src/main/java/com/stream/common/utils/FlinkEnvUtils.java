package com.stream.common.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @TODO:用途：Apache Flink 环境配置工具类。
 *   功能：
 *     创建 StreamExecutionEnvironment。
 *     配置状态后端（如 RocksDB）、检查点（Checkpoint）。
 *   典型场景：实时流处理任务初始化。
 */
public class FlinkEnvUtils {

    public static StreamExecutionEnvironment getFlinkRuntimeEnv(){
        if (CommonUtils.isIdeaEnv()){
            System.err.println("Action Local Env");
            return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        }
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
