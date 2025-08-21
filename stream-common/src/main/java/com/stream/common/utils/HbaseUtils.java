package com.stream.common.utils;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.crypto.SecureUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.stream.common.domain.HBaseInfo;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;

import static org.apache.hadoop.hbase.CellUtil.cloneQualifier;
import static org.apache.hadoop.hbase.CellUtil.cloneValue;

/**
 *  HBase 工具类
 */
public class HbaseUtils {
    private Connection connection;
    private static final Logger LOG = LoggerFactory.getLogger(HbaseUtils.class.getName());

    /**
     * 构造方法：初始化HBase连接
     * @param zookeeper_quorum ZooKeeper集群地址（逗号分隔）
     * @throws Exception 连接异常
     */
    public HbaseUtils(String zookeeper_quorum) throws Exception {
        org.apache.hadoop.conf.Configuration entries = HBaseConfiguration.create();
        entries.set(HConstants.ZOOKEEPER_QUORUM, zookeeper_quorum);
        // setting hbase "hbase.rpc.timeout" and "hbase.client.scanner.timeout" Avoidance scan timeout
        // 设置HBase的RPC超时和扫描超时，避免扫描超时
        entries.set(HConstants.HBASE_RPC_TIMEOUT_KEY,"1800000");
        entries.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,"1800000");
        // setting hbase "hbase.hregion.memstore.flush.size" buffer flush
        // 设置HBase的memstore刷新大小
        entries.set(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,"128M");
        entries.set("hbase.incremental.wal","true");
        entries.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,"3600000");
//        entries.set(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,"1200000");
        this.connection = ConnectionFactory.createConnection(entries);
    }

    /**
     * 获取HBase连接对象
     * @return Connection对象
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * 向HBase表插入数据（通过BufferedMutator批量插入）
     * @param rowKey 行键
     * @param value 包含列名和值的JSON对象
     * @param mutator 批量写入器
     * @throws IOException 写入异常
     */
    public static void put(String rowKey, JSONObject value, BufferedMutator mutator) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
        }
        mutator.mutate(put);
    }

    /**
     * 构建Put对象（未执行实际插入）
     * @param rowKey 行键
     * @param value 包含列名和值的JSON对象
     */
    public static void put(String rowKey, JSONObject value){
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
        }
    }

    /**
     * 创建HBase表
     * @param nameSpace 命名空间
     * @param tableName 表名
     * @param columnFamily 列族（可变参数）
     * @return 表是否创建成功
     * @throws Exception 异常
     */
    public boolean createTable(String nameSpace,String tableName, String... columnFamily) throws Exception {
        boolean b = tableIsExists(tableName);
        if (b) {
            return true;
        }
        Admin admin = connection.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace,tableName));
        if (columnFamily.length > 0) {
            for (String s : columnFamily) {
                ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(s.getBytes()).setCompressionType(Compression.Algorithm.SNAPPY).build();
                System.err.println("构建表列族：" + s);
                tableDescriptorBuilder.setColumnFamily(build);
            }
        } else {
            ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).setCompressionType(Compression.Algorithm.SNAPPY).build();
            System.err.println("构建表列族：info");
            tableDescriptorBuilder.setColumnFamily(build);
        }
        TableDescriptor build = tableDescriptorBuilder
                .build();
        admin.createTable(build);
        admin.close();
        LOG.info("Create Table {}",tableName);
        return tableIsExists(tableName);
    }

    /**
     * 检查表是否存在
     * @param tableName 表名
     * @return 表是否存在
     * @throws Exception 异常
     */
    public boolean tableIsExists(String tableName) throws Exception {
        Thread.sleep(1000);
        Admin admin = connection.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        System.err.println("表 ：" + tableName + (b ? " 存在" : " 不存在"));
        return b;
    }

    /**
     * 获取指定命名空间下的所有表
     * @param nameSpace 命名空间
     * @throws IOException 异常
     */
    public void getHbaseNameSpaceAllTablesList(String nameSpace) throws IOException {
        Admin admin = connection.getAdmin();
        TableName[] tableNamesByNamespace = admin.listTableNamesByNamespace(nameSpace);
        ArrayList<TableName> tableNames = new ArrayList<>(Arrays.asList(tableNamesByNamespace));
        if (!tableNames.isEmpty()){
            for (TableName tableName : tableNames) {
                System.err.println("table -> "+tableName);
            }
        }
    }

    /**
     * 删除HBase表
     * @param tableName 表名
     * @return 删除后表是否仍存在（成功删除返回false）
     * @throws Exception 异常
     */
    public boolean deleteTable(String tableName) throws Exception {
        boolean b = tableIsExists(tableName);
        if (!b) {
            return false;
        }
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        System.err.println("禁用表：" + tableName);
        admin.deleteTable(TableName.valueOf(tableName));
        System.err.println("删除表 ：" + tableName);
        return tableIsExists(tableName);
    }

    /**
     * 根据行键获取数据（字符串形式）
     * @param tableName 表名
     * @param rowkey 行键
     * @return 结果字符串
     * @throws IOException 异常
     */
    public String getString(String tableName, String rowkey) throws IOException {
        Get get = new Get(rowkey.getBytes());
        Table table = connection.getTable(TableName.valueOf(tableName));
        Result result = table.get(get);
        return result.toString();
    }

    /**
     * 检查连接是否有效
     * @return 连接是否未关闭
     */
    public boolean isConnect() {
        return !connection.isClosed();
    }

    /**
     * 扫描表中所有数据（带数量限制）
     * @param tableName 表名
     * @param limit 最大返回条数
     * @return 包含数据的JSON对象列表
     * @throws Exception 异常
     */
    public ArrayList<JSONObject> getAll(String tableName, long limit) throws Exception {
        long l = System.currentTimeMillis();
        if (!this.tableIsExists(tableName)) {
            throw new NullPointerException("表不存在");
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setLimit(Math.toIntExact(limit));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
//        List list1 = IteratorUtils.toList(iterator);
        ArrayList<JSONObject> list = new ArrayList<>();
        while (iterator.hasNext()) {
            Result next = iterator.next();
            JSONObject js = new JSONObject();
            next.listCells().forEach(cell -> {
                js.put("row_key", Bytes.toString(next.getRow()));
                js.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            });
            list.add(js);
        }
        long l1 = System.currentTimeMillis();
        System.err.println("耗时 " + (l1 - l));
        return list;
    }


    /**
     * 批量删除指定行键的数据
     * @param tableName 表名
     * @param deletes Delete对象列表
     * @throws IOException 异常
     */
    public void deleteByRowkeys(String tableName, ArrayList<Delete> deletes) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.delete(deletes);
    }


    /**
     * 删除指定快照
     * @param snapshotName 快照名
     * @return 操作结果信息
     * @throws IOException 异常
     */
    public String delSnapshots(String snapshotName) throws IOException {
        for (SnapshotDescription listSnapshot : connection.getAdmin().listSnapshots()) {
            if (!listSnapshot.getName().isEmpty() && listSnapshot.getName().equals(snapshotName)){
                connection.getAdmin().deleteSnapshot(snapshotName);
                return "delete of -> "+ snapshotName;
            }
        }
        return "The "+snapshotName+" does not Exist !";
    }


    /**
     * 统计表的行数
     * @param tableName 表名
     * @return 包含行数和耗时的信息字符串
     * @throws IOException 异常
     */
    public String getTableRows(String tableName) throws IOException {
        long rowCount = 0;
        long startTime = System.currentTimeMillis();
        TableName tableName1 = TableName.valueOf(tableName);
        Table table = connection.getTable(tableName1);
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            rowCount += r.size();
        }
        long stopTime = System.currentTimeMillis();
        return "表 -> "+tableName + "共计: "+rowCount +" 条"+" , 统计耗时 -> "+(stopTime - startTime);
    }

    /**
     * 删除指定命名空间下的所有表
     * @param nameSpace 命名空间
     */
    @SneakyThrows
    public void dropHbaseNameSpace(String nameSpace){
        Admin admin = connection.getAdmin();
        TableName[] tableNamesByNamespace = admin.listTableNamesByNamespace(nameSpace);
        ArrayList<TableName> tableNames = new ArrayList<>(Arrays.asList(tableNamesByNamespace));
        if (!tableNames.isEmpty()){
            for (TableName tableName : tableNames) {
                Table table = connection.getTable(tableName);
                admin.disableTable(table.getName());
                admin.deleteTable(tableName);
                System.err.println("del -> "+table.getName());
            }
        }
    }

    /**
     * 主方法：测试HBase工具类功能
     * @param args 命令行参数
     */
    @SneakyThrows
    public static void main(String[] args) {
        // 设置Hadoop的用户名为root，解决HBase操作时的权限问题
        // 当程序访问Hadoop/HBase集群时，会使用该用户身份进行认证
        System.setProperty("HADOOP_USER_NAME","root");
        // 初始化HBase工具类实例，参数为ZooKeeper集群的主机名（cdh01,cdh02,cdh03）
        // HBase通过ZooKeeper进行集群协调，因此需要指定ZooKeeper的地址
        HbaseUtils hbaseUtils = new HbaseUtils("cdh01,cdh02,cdh03");

        //TODO: 测试HBase工具类功能

        //TODO:  1. 删除指定命名空间下的所有表
        // 注意：该操作会先禁用命名空间下的所有表，再删除它们，谨慎执行
        // hbaseUtils.dropHbaseNameSpace("GMALL_FLINK_2207");

        //TODO:  2. 检查表是否存在
        // 输出结果为true（存在）或false（不存在），并在控制台打印提示信息
        // System.err.println(hbaseUtils.tableIsExists("realtime_v2:dim_user_info"));

        //TODO:  3. 删除指定表
        // 操作流程：先检查表是否存在，存在则禁用表，然后删除表
//         hbaseUtils.deleteTable("ns_zxn:dim_base_category1");

        //TODO:  4. 获取指定命名空间下的所有表名
        // 遍历并打印该命名空间下的所有表信息
        // hbaseUtils.getHbaseNameSpaceAllTablesList("realtime_v2");

    }
}