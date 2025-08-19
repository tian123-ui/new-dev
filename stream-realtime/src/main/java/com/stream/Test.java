package com.stream;

import com.stream.common.utils.ConfigUtils;
import lombok.SneakyThrows;


public class Test {
    @SneakyThrows
    public static void main(String[] args) {
        String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
        System.err.println(kafka_botstrap_servers);
    }

}
