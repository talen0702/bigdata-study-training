package com.talen.hbase.util;

import com.talen.hbase.ColBean;
import com.talen.hbase.RowBean;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * HBase 工具类
 */
@Data
@Slf4j
public class HBaseUtil {

    private static Configuration conf;

    private static Connection conn;

    private static String zkHost;

    private static String parent;

    public static void init(String zkH) {
        try {
            zkHost = zkH;
            if (conf == null) {
                conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", zkH);
                //conf.set("zookeeper.znode.parent", pt);
            }
        } catch (Exception e) {
            log.error("HBase Configuration Initialization failure !");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 获得链接
     *
     * @return
     */
    public static synchronized Connection getConnection() {
        try {
            return conn == null || conn.isClosed() ? conn = ConnectionFactory.createConnection(conf) : conn;
        } catch (IOException e) {
            log.error("HBase 建立链接失败! ");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    /**
     * 获取  Table
     *
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) {
        try {
            return getConnection().getTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            log.error("Obtain Table failure !", e);
        }
        throw new RuntimeException("获取表名异常");
    }

    public static void createTableIfNotExist(String nameSpace, String tableName, String[] columnFamilys) {
        Connection con = getConnection();
        try {
            Admin admin = con.getAdmin();
            TableName tName = TableName.valueOf(nameSpace, tableName);
            if (tableName != null) {
                List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<ColumnFamilyDescriptor>();
                for (String columnFamily : columnFamilys) {
                    ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes(StandardCharsets.UTF_8))
                            .setCompressionType(Compression.Algorithm.SNAPPY)
                            .build();
                    columnFamilyDescriptors.add(columnFamilyDescriptor);
                }
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tName)
                        .setColumnFamilies(columnFamilyDescriptors)
                        .build();
                admin.createTable(tableDescriptor);
                admin.close();
            }
        } catch (Exception e) {
            log.error("Obtain Table failure !", e);
            e.printStackTrace();
        } finally {
            closeConnect(con);
        }
    }

    /**
     * 获取单条数据
     *
     * @param tablename
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static Result getRow(String tablename, byte[] rowKey) {
        Table table = getTable(tablename);
        Result rs = null;
        if (table != null) {
            try {
                Get g = new Get(rowKey);
                rs = table.get(g);
            } catch (IOException e) {
                log.error("getRow failure !", e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("getRow failure !", e);
                }
            }
        }
        return rs;
    }


    /**
     * 异步往指定表添加数据
     *
     * @param tablename 表名
     * @param puts      需要添加的数据
     * @throws IOException
     */
    public static long put(String tablename, List<Put> puts) throws IOException {
        long currentTime = System.currentTimeMillis();
        Connection conn = getConnection();
        final BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                log.error("Failed to sent put " + e.getRow(i) + ".");
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename))
                .listener(listener);
        params.writeBufferSize(24 * 1024 * 1024);
        try (BufferedMutator mutator = conn.getBufferedMutator(params)) {
            mutator.mutate(puts);
            mutator.flush();
        } finally {
            closeConnect(conn);
        }
        return System.currentTimeMillis() - currentTime;
    }

    public static long put(String tableName, List<Put> puts, Connection conn) throws Exception {
        long currentTime = System.currentTimeMillis();
        final BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                log.error("Failed to sent put " + e.getRow(i) + ".");
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
                .listener(listener);
        params.writeBufferSize(24 * 1024 * 1024);
        try (BufferedMutator mutator = conn.getBufferedMutator(params)) {
            mutator.mutate(puts);
            mutator.flush();
        }
        return System.currentTimeMillis() - currentTime;
    }

    public static synchronized long put(List<Put> puts, BufferedMutator mutator) throws Exception {
        long currentTime = System.currentTimeMillis();
        mutator.mutate(puts);
        return System.currentTimeMillis() - currentTime;
    }

    public static long putRows(String tableName, List<Put> puts) {
        long currentTime = System.currentTimeMillis();
        try (Table table = Optional.ofNullable(getTable(tableName))
                .orElseThrow(() -> new RuntimeException("表不存在,表名: " + tableName))) {
            table.put(puts);
        } catch (Exception e) {
            log.error("Hbase写入异常", e);
        }
        return System.currentTimeMillis() - currentTime;
    }


    // 根据rowKey前缀及字段名删除表数据
    public static void delete(String oneInningTable, String type_key, String data_key) throws IOException {
        Table table = HBaseUtil.getTable(oneInningTable);
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(type_key.getBytes()));
        if (StringUtils.isNotBlank(data_key)) {
            scan.addColumn("F1".getBytes(), data_key.getBytes());
        }

        ResultScanner results = table.getScanner(scan);

        Iterator<Result> it = results.iterator();
        List<Delete> list = new ArrayList<>();
        while (it.hasNext()) {
            Result delete = it.next();
            Delete d = new Delete(delete.getRow());
            if (StringUtils.isNotBlank(data_key)) {
                d.addColumn("F1".getBytes(), data_key.getBytes());
            }
            list.add(d);
        }
        table.delete(list);
    }

    public static Map<String, Map<String, String>> scan(String prefix, String hbaseTable) throws IOException {

        Table table = HBaseUtil.getTable(hbaseTable);
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(prefix.getBytes()));

        ResultScanner results = table.getScanner(scan);

        Map<String, Map<String, String>> scanResult = new HashMap<>();
        for (Result res : results) {
            Map<String, String> map = new HashMap<>();
            for (Cell cell : res.rawCells()) {
                String dataKet = Bytes.toString(CellUtil.cloneQualifier(cell));
                String dataValue = Bytes.toString(CellUtil.cloneValue(cell));
                map.put(dataKet, dataValue);
            }
            scanResult.put(Bytes.toString(res.getRow()), map);
        }
        return scanResult;
    }


    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public static void closeConnect(Connection conn) {
        if (null != conn) {
            try {
                conn.close();
            } catch (Exception e) {
                log.error("closeConnect failure !", e);
            }
        }
    }

    /**
     * 根据family和列名从Result里取数据
     *
     * @param
     * @return
     */
    public static String getColumnValue(Result result, String family, String columnName) {
        String columnValue = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(columnName)));
        if (StringUtils.isBlank(columnValue) || "null".equals(columnValue) || "NULL".equals(columnValue) || "Null".equals(columnValue)) {
            return null;
        }
        return columnValue;
    }


    public static void createNameSpace(String namespace) {
        // 获取连接对象
        // 获取操作对象
        try (Connection conn = getConnection(); Admin admin = conn.getAdmin()) {

            //获取namespace对象
            NamespaceDescriptor hbase = admin.getNamespaceDescriptor(namespace);
            //判断这个namespace是否存在
            if (hbase != null) {
                //如果这个namespace存在 ,遍历namespace,将里面的table都取出来
                TableName[] hbase01s = admin.listTableNamesByNamespace(namespace);
                for (TableName tableName : hbase01s) {
                    // 禁用表(禁用表后,才能对表进行删除/更新等操作)
                    admin.disableTable(tableName);
                    // 删除表
                    admin.deleteTable(tableName);
                }
                // 删除 namespace
                admin.deleteNamespace(namespace);
            }
            // 获取名称空间的构建器对象
            NamespaceDescriptor.Builder hbase1 = NamespaceDescriptor.create(namespace);

            // 构造器对象调用构建方法,返回/获取构建对象
            NamespaceDescriptor build = hbase1.build();
            //创建名称空间
            admin.createNamespace(build);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 添加多条记录
     */
    public static void addRecord(String nameSpace, String tableName, List<RowBean> rowList) {
        TableName tName = TableName.valueOf(nameSpace, tableName);
        try (Connection conn = getConnection(); Table table = conn.getTable(tName)) {
            List<Put> puts = new ArrayList<>(rowList.size());
            for (RowBean rowBean : rowList) {
                for (String columnFamily : rowBean.getColumnFamilys().keySet()) {
                    for (ColBean colBean : rowBean.getColumnFamilys().get(columnFamily)) {
                        Put put = new Put(Bytes.toBytes(rowBean.getRowKey()));
                        put.addColumn(columnFamily.getBytes(StandardCharsets.UTF_8),
                                colBean.getColName() == null ? null : colBean.getColName().getBytes(StandardCharsets.UTF_8),
                                colBean.getColValue().getBytes(StandardCharsets.UTF_8));
                        puts.add(put);
                    }
                }

            }
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除一张表
     *
     * @param tableName
     */
    public static void dropTable(String nameSpace, String tableName) {
        TableName tName = TableName.valueOf(nameSpace, tableName);
        try (Connection conn = getConnection();) {
            Admin admin = conn.getAdmin();
            admin.disableTable(tName);
            admin.deleteTable(tName);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
