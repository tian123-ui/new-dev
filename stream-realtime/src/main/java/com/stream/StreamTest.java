package com.stream;

import lombok.SneakyThrows;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.nio.charset.StandardCharsets;


public class StreamTest {
    @SneakyThrows
    public static void main(String[] args) {

        System.err.println(MD5Hash.getMD5AsHex("15".getBytes(StandardCharsets.UTF_8)));



    }
}
