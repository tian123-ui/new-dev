package com.stream.common.domain;

import java.io.Serializable;

public class HdfsInfo implements Serializable {

    private String hdfsUrl; //根路径地址
    private boolean hdfsNeedPartition; //  是否启用分区存储
    private int hdfsPartitionMode; //分区模式标识
    private String hdfsPartitionField; //分区依据字段

    /**
     * 默认构造函数
     */
    public HdfsInfo() {
    }

    /**
     * 全参构造函数
     *
     * @param hdfsUrl 根路径地址
     * @param hdfsNeedPartition 是否启用分区
     * @param hdfsPartitionMode 分区模式标识（0=天,1=月,2=年,3=业务字段）
     * @param hdfsPartitionField 分区依据字段
     */
    public HdfsInfo(String hdfsUrl, boolean hdfsNeedPartition, int hdfsPartitionMode, String hdfsPartitionField) {
        this.hdfsUrl = hdfsUrl;
        this.hdfsNeedPartition = hdfsNeedPartition;
        this.hdfsPartitionMode = hdfsPartitionMode;
        this.hdfsPartitionField = hdfsPartitionField;
    }

    /**
     * 获取根路径地址
     * @return 根路径地址
     */
    public String getHdfsUrl() {
        return hdfsUrl;
    }

    /**
     * 设置根路径地址
     * @param hdfsUrl 根路径地址
     */
    public void setHdfsUrl(String hdfsUrl) {
        this.hdfsUrl = hdfsUrl;
    }

    /**
     * 获取是否启用分区
     * @return 是否启用分区
     */
    public boolean isHdfsNeedPartition() {
        return hdfsNeedPartition;
    }

    /**
     * 设置是否启用分区
     * @param hdfsNeedPartition 是否启用分区
     */
    public void setHdfsNeedPartition(boolean hdfsNeedPartition) {
        this.hdfsNeedPartition = hdfsNeedPartition;
    }

    /**
     * 获取分区模式标识
     * @return 分区模式标识
     */
    public int getHdfsPartitionMode() {
        return hdfsPartitionMode;
    }

    /**
     * 设置分区模式标识
     * @param hdfsPartitionMode 分区模式标识
     */
    public void setHdfsPartitionMode(int hdfsPartitionMode) {
        this.hdfsPartitionMode = hdfsPartitionMode;
    }

    /**
     * 获取分区依据字段
     * @return 分区依据字段
     */
    public String getHdfsPartitionField() {
        return hdfsPartitionField;
    }

    /**
     * 设置分区依据字段
     * @param hdfsPartitionField 分区依据字段
     */
    public void setHdfsPartitionField(String hdfsPartitionField) {
        this.hdfsPartitionField = hdfsPartitionField;
    }

    /**
     * 生成对象的字符串表示（用于日志调试）
     * @return 式化字符串，包含所有 HDFS 配置参数
     */
    @Override
    public String toString() {
        return "HdfsInfo{" +
                "hdfsUrl='" + hdfsUrl + '\'' +
                ", hdfsNeedPartition=" + hdfsNeedPartition +
                ", hdfsPartitionMode=" + hdfsPartitionMode +
                ", hdfsPartitionField='" + hdfsPartitionField + '\'' +
                '}';
    }
}
