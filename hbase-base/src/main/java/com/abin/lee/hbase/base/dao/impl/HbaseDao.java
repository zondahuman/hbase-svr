package com.abin.lee.hbase.base.dao.impl;


import com.abin.lee.hbase.base.dao.IHBaseDao;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;



/**
 * Created by abin on 2017/12/16 2017/12/16.
 * hbase-svr
 * com.abin.lee.hbase.base.dao.impl
 */
public class HbaseDao implements IHBaseDao {
    public final static String COLENDCHAR = String
            .valueOf(KeyValue.COLUMN_FAMILY_DELIMITER);// ":" 列簇和列之间的分隔符

    HBaseConfiguration conf;
    HBaseAdmin admin;

    public HBaseConfiguration getConf() {
        return conf;
    }

    public void setConf(HBaseConfiguration conf) {
        this.conf = conf;
    }

    public HBaseAdmin getAdmin() {
        return admin;
    }

    public void setAdmin(HBaseAdmin admin) {
        this.admin = admin;
    }

    public void createHTable(String tableName, String[] columns)
            throws IOException {
        try {
            if (admin.tableExists(tableName))
                return;// 判断表是否已经存在
            HTableDescriptor htdesc = this.createHTDesc(tableName);
            for (int i = 0; i < columns.length; i++) {
                String colName = columns[i];
                this.addFamily(htdesc, colName, false);
            }
            admin.createTable(htdesc);
        } catch (IOException e) {
            throw e;
        }
    }

    public void createHTable(String tableName) throws IOException {
        try {
            if (admin.tableExists(tableName))
                return;// 判断表是否已经存在
            HTableDescriptor htdesc = this.createHTDesc(tableName);
            admin.createTable(htdesc);
        } catch (IOException e) {
            throw e;
        }
    }

    public void deleteColumn(String tableName, String rowID, String colName,
                             String cluster) throws IOException {
        try {
            Delete del = new Delete(rowID.getBytes());
//            if (cluster == null || "".equals(cluster))
//                del.deleteColumn(colName.getBytes());
//            else
//                del.addColumn(colName.getBytes(), cluster.getBytes());
            HTable hTable = this.getHTable(tableName);
            hTable.delete(del);
        } catch (IOException e) {
            throw e;
        }
    }

    public Map<String, String> getColumnValue(String tableName, String colName,
                                              String cluster) throws IOException {
        ResultScanner scanner = null;
        try {
            HTable hTable = this.getHTable(tableName);
            scanner = hTable.getScanner(colName.getBytes(), cluster.getBytes());
            Result rowResult = scanner.next();
            Map<String, String> resultMap = new HashMap<String, String>();
            String row;
            while (rowResult != null) {
                row = new String(rowResult.getRow());
                resultMap.put(row, new String(rowResult.getValue(colName
                        .getBytes(), cluster.getBytes())));
                rowResult = scanner.next();
            }
            return resultMap;
        } catch (IOException e) {
            throw e;
        } finally {
            if (scanner != null) {
                scanner.close();// 一定要关闭
            }
        }
    }

    public String getValue(String tableName, String rowID, String colName,
                           String cluster) throws IOException {
        try {
            HTable hTable = this.getHTable(tableName);
            Get get = new Get(rowID.getBytes());
            Result result = hTable.get(get);
            byte[] b = result.getValue(colName.getBytes(), cluster.getBytes());
            if (b == null)
                return "";
            else
                return new String(b);
        } catch (IOException e) {
            throw e;
        }
    }

    public void insertAndUpdate(String tableName, String row, String family,
                                String qualifier, String value) throws IOException {
        HTable table = this.getHTable(tableName);
        Put p = new Put(Bytes.toBytes(row));
        p.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                .toBytes(value));
        table.put(p);

    }

    public void removeFamily(String tableName, String colName)
            throws IOException {
        try {
            String tmp = this.fixColName(colName);
            if (admin.isTableAvailable(tableName))
                admin.disableTable(tableName);
            this.admin.deleteColumn(tableName, tmp);
            this.admin.enableTable(tableName);
        } catch (IOException e) {
            throw e;
        }
    }

    /**
     * 给表添加列,此时不带列族
     *
     * @param htdesc
     * @param colName
     * @param readonly
     *            是否只读
     * @throws Exception
     */
    private void addFamily(HTableDescriptor htdesc, String colName,
                           final boolean readonly) {
        htdesc.addFamily(this.createHCDesc(colName));
        htdesc.setReadOnly(readonly);
    }

    /**
     * 创建列的描述,添加后，该列会有一个冒号的后缀，用于存储(列)族, 将来如果需要扩展，那么就在该列后加入(列)族
     *
     * @param colName
     * @return
     */
    private HColumnDescriptor createHCDesc(String colName) {
        String tmp = this.fixColName(colName);
        byte[] colNameByte = Bytes.toBytes(tmp);
        return new HColumnDescriptor(colNameByte);
    }

    /**
     * 针对hbase的列的特殊情况进行处理,列的情况: course: or course:math, 就是要么带列族，要么不带列族(以冒号结尾)
     *
     * @param colName
     *            列
     * @param cluster
     *            列族
     * @return
     */
    private String fixColName(String colName, String cluster) {
        if (cluster != null && cluster.trim().length() > 0
                && colName.endsWith(cluster)) {
            return colName;
        }
        String tmp = colName;
        int index = colName.indexOf(COLENDCHAR);
        // int leng = colName.length();
        if (index == -1) {
            tmp += COLENDCHAR;
        }
        // 直接加入列族
        if (cluster != null && cluster.trim().length() > 0) {
            tmp += cluster;
        }
        return tmp;
    }

    private String fixColName(String colName) {
        return this.fixColName(colName, null);
    }

    /**
     * 创建表的描述
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    private HTableDescriptor createHTDesc(final String tableName) {
        return new HTableDescriptor(tableName);
    }

    /**
     * 取得某个表
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    private HTable getHTable(String tableName) throws IOException {
        try {
            return new HTable(conf, tableName);
        } catch (IOException e) {
            throw e;
        }
    }

}