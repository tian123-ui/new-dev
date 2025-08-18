package com.stream.common.domain;

import java.io.Serializable;

/**
 * HBase 表配置实体类，用于存储以太坊相关数据在 HBase 中的表名配置。
 *
 * @className: HdfsInfo
 * @description 封装HDFS信息 Bean
 */
public class HBaseTable implements Serializable {

    private String ethBlock; //存储以太坊区块数据的 HBase 表名（按区块高度索引）
    private String ethTranNormalByHash; //存储普通交易数据的 HBase 表名（主键为交易哈希，用于精确查询单笔交易）
    private String ethTranInternalByHash; //存储内部交易数据的 HBase 表名（主键为交易哈希，用于查询单笔交易的内部调用）
    private String ethTranNormalByAddress; //存储普通交易数据的 HBase 表名（主键为地址+时间戳，用于查询某地址的所有普通交易）
    private String ethTranInternalByAddress; //存储内部交易数据的 HBase 表名（主键为地址+时间戳，用于查询某地址的所有内部交易）

    /**
     * 默认构造函数（用于反射或序列化框架）
     */
    public HBaseTable() {

    }

    /**
     * 构造函数
     * @param ethBlock              存储以太坊区块数据的 HBase 表名（按区块高度索引）
     * @param ethTranNormalByHash   存储普通交易数据的 HBase 表名（主键为交易哈希，用于精确查询单笔交易）
     * @param ethTranInternalByHash 存储内部交易数据的 HBase 表名（主键为交易哈希，用于查询单笔交易的内部调用）
     * @param ethTranNormalByAddress 存储普通交易数据的 HBase 表名（主键为地址+时间戳，用于查询某地址的所有
    **/
    public HBaseTable(String ethBlock, String ethTranNormalByHash, String ethTranInternalByHash,
            String ethTranNormalByAddress, String ethTranInternalByAddress) {
        this.ethBlock = ethBlock;
        this.ethTranNormalByHash = ethTranNormalByHash;
        this.ethTranInternalByHash = ethTranInternalByHash;
        this.ethTranNormalByAddress = ethTranNormalByAddress;
        this.ethTranInternalByAddress = ethTranInternalByAddress;
    }

    /**
     * 获取存储以太坊区块数据的 HBase 表名（按区块高度索引）
     * @return 存储以太坊区块数据的 HBase 表名（按区块高度索引）
     */
    public String getEthBlock() {
        return ethBlock;
    }

    /**
     * 设置存储以太坊区块数据的 HBase 表名（按区块高度索引）
     * @param ethBlock 存储以太坊区块数据的 HBase 表名（按区块高度索引）
     */
    public void setEthBlock(String ethBlock) {
        this.ethBlock = ethBlock;
    }

    /**
     * 获取普通交易（按哈希查询）表名
     * @return HBase 表名（如 "eth_tran_normal_by_hash"）
     */
    public String getEthTranNormalByHash() {
        return ethTranNormalByHash;
    }

     /**
     * 设置普通交易（按哈希查询）表名
     * @param ethTranNormalByHash 普通交易（按哈希查询）表名
     */
    public void setEthTranNormalByHash(String ethTranNormalByHash) {
        this.ethTranNormalByHash = ethTranNormalByHash;
    }

    /**
     * 获取内部交易（按哈希查询）表名
     * @return HBase 表名（如 "eth_tran_internal_by_hash"）
     */
    public String getEthTranInternalByHash() {
        return ethTranInternalByHash;
    }

    /**
     * 设置内部交易（按哈希查询）表名
     * @param ethTranInternalByHash 内部交易（按哈希查询）表名
     */
    public void setEthTranInternalByHash(String ethTranInternalByHash) {
        this.ethTranInternalByHash = ethTranInternalByHash;
    }

    /**
     * 获取普通交易（按地址查询）表名
     * @return HBase 表名（如 "eth_tran_normal_by_address"）
     */
    public String getEthTranNormalByAddress() {
        return ethTranNormalByAddress;
    }

    /**
     * 设置普通交易（按地址查询）表名
     * @param ethTranNormalByAddress 普通交易（按地址查询）表名
     */
    public void setEthTranNormalByAddress(String ethTranNormalByAddress) {
        this.ethTranNormalByAddress = ethTranNormalByAddress;
    }

    /**
     * 获取内部交易（按地址查询）表名
     * @return HBase 表名（如 "eth_tran_internal_by_address"）
     */
    public String getEthTranInternalByAddress() {
        return ethTranInternalByAddress;
    }

    /**
     * 获取内部交易（按地址查询）表名
     * @param ethTranInternalByAddress 内部交易（按地址查询）表名
     */
    public void setEthTranInternalByAddress(String ethTranInternalByAddress) {
        this.ethTranInternalByAddress = ethTranInternalByAddress;
    }

    /**
     * 生成对象的字符串表示（用于日志调试）
     * 重写 toString 方法，用于打印对象信息
     * @return 表名信息 格式化字符串，包含所有表名配置
     */
    @Override
    public String toString() {
        return "HBaseTable{" +
                "ethBlock='" + ethBlock + '\'' +
                ", ethTranNormalByHash='" + ethTranNormalByHash + '\'' +
                ", ethTranInternalByHash='" + ethTranInternalByHash + '\'' +
                ", ethTranNormalByAddress='" + ethTranNormalByAddress + '\'' +
                ", ethTranInternalByAddress='" + ethTranInternalByAddress + '\'' +
                '}';
    }
}
