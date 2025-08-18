package com.stream.common.utils;

import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;

/**
 * TODO:用途：文件系统操作工具类。
 *  功能：
 *     文件读写、复制、删除、压缩。
 *     目录遍历、大小计算。
 *  典型场景：日志文件处理、临时文件管理。
 */
public class FileUtils {

    public static long getFileLastTime(String filePath){
        long time = 0;
        File file = new File(filePath);
        if (file.exists()){
            time = file.lastModified();
        }
        return time;
    }

    public static void sink2File(String path, String data){
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path, false))){
            writer.write(data);
            writer.newLine();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String getFileFirstLineData(String path){
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            return reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static ArrayList<JSONObject> readFileData(String path){
        ArrayList<JSONObject> res = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))){
            String line ;
            while ((line = reader.readLine()) != null){
                JSONObject jsonObject = JSONObject.parseObject(line);
                res.add(jsonObject);
            }
        }catch (IOException ioException){
            ioException.printStackTrace();
        }
        return res;
    }

    public static String getResourceDicPath(){
        ClassLoader classLoader = FileUtils.class.getClassLoader();
        URL resource = classLoader.getResource("");
        if (resource != null){
            return resource.getPath();
        }
        return null;
    }

    public static void main(String[] args) {
        System.err.println(getResourceDicPath());
    }


}
