package com.stream.common.domain;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HBaseInfo implements Serializable {


    private String address; //hbase地址
    private String port; //hbase端口
    private List<HBaseTable> tableList = new ArrayList<>(); //hbase表信息

    /**
     * hbase表信息
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HBaseTable implements Serializable {

        private String tableName; //hbase表名
        private List<RowKey> rowKeyList; //hbase表行键信息
        private String tableFamily = "info"; //hbase表列族

        /**
         * 构造函数  通过可变参数创建行键配置
         * @param tableName 表名
         * @param rowKeyList 行键字段配置（可传入多个 RowKeyItem）
         *
         */
        public HBaseTable(String tableName, RowKey... rowKeyList) {
            this.tableName = tableName;
            this.rowKeyList = Arrays.asList(rowKeyList);
        }

        /**
         * 构造函数  通过list创建行键配置
         * @param tableName 表名
         * @param rowKeyList 行键字段配置（可传入多个 RowKeyItem）
         *
         */
        public HBaseTable(String tableName, List<RowKey> rowKeyList) {
            this.tableName = tableName;
            this.rowKeyList = rowKeyList;
        }
    }

    /**
     * hbase表行键信息
     */
    @Data
    @NoArgsConstructor
    public static class RowKey implements Serializable {

        private List<RowKeyItem> rowKeyItemList = new ArrayList<>(); //行键字段信息（按顺序组成最终行键）
        private String rowKeyItemFieldsSeparator = ""; //行键字段分隔符 （默认空字符串，如需分隔建议用 "_"）
        private Boolean rowKeyNeedMd5 = false; //行键是否需要 MD5 加密  (true: 生成 MD5 哈希值，false: 原始字符串）

        /**
         * 构造函数  通过可变参数创建行键字段信息
         * @param fieldName 行键字段名
         *
         */
        public RowKey(String fieldName) {
            rowKeyItemList.add(new RowKeyItem(fieldName, false));
        }

        /**
         * 构造函数  通过可变参数创建行键字段信息
         * @param fieldName 行键字段名
         * @param rowKeyItemNeedMd5 行键字段是否需要MD5
         *
         */
        public RowKey(String fieldName, Boolean rowKeyItemNeedMd5) {
            rowKeyItemList.add(new RowKeyItem(fieldName, rowKeyItemNeedMd5));
        }

        /**
         * 构造函数  通过可变参数创建行键字段信息
         * @param list 行键字段信息
         *
         */
        public RowKey(RowKeyItem... list) {
            this.rowKeyItemList.addAll(Arrays.asList(list));
        }
    }

    /**
     * 行键字段配置单元
     * hbase表行键字段信息
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RowKeyItem implements Serializable {

        private String fieldName; //
        private Boolean rowKeyItemNeedMd5; //行键字段是否需要MD5
    }

}
