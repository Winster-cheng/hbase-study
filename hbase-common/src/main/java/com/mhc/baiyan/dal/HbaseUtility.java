package com.mhc.baiyan.dal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Auther: peilongcheng
 * @Date: 2019/5/6 17:21
 * @Description: HBASE增删改查操作
 */
public class HbaseUtility {
    public static Configuration conf;
    public static Connection conn;
    /**
     * 类级别的初始化，只是在类加载的时候做一次 配置zookeeper的端口2181
     * 配置zookeeper的仲裁主机名centos，如果有多个机器，主机名间用冒号隔开 配置hbase master
     * 还有一种方式是new一个configuration对象，然后用addresource方法去添加xml配置文件 但是像这样显式的配置是会覆盖xml里的配置的
     */
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "172.21.10.53");
        conf.set("hbase.master", "172.21.10.53:16000");
//        conf.addResource("core-site.xml");
//        conf.addResource("hbase-site.xml");
//        conf.addResource("hdfs-site.xml");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 建表，建列族
     *
     * @param tablename,
     * @param ColumnFamilys
     *            NamespaceDescriptor:维护命名空间的信息,但是namespace，一般用shell来建立
     *            Admin:提供了一个接口来管理 HBase 数据库的表信息
     *            HTableDescriptor:维护了表的名字及其对应表的列族,通过HTableDescriptor对象设置表的特性
     *            HColumnDescriptor:维护着关于列族的信息,可以通过HColumnDescriptor对象设置列族的特性
     */
    public static void createtable(String tablename, String... ColumnFamilys) throws IOException {
        Admin admin = conn.getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tablename));
        for (String family : ColumnFamilys) {
            HColumnDescriptor columnfamily = new HColumnDescriptor(family);
            table.addFamily(columnfamily);
        }

        if (admin.tableExists(TableName.valueOf(tablename))) {
            System.out.println("Table Exists");
        } else {
            admin.createTable(table);
            System.out.println("Table Created");
            admin.close();
        }
    }

    /**
     * 插入数据,当指定rowkey已经存在，则会覆盖掉之前的旧数据
     *
     * @param tablename,
     * @param rowkey,
     * @param ColumnFamilys,
     * @param columns,@values
     *            Table:用于与单个HBase表通信 Put:用来对单个行执行添加操作
     */
    public static void insertdata(String tablename, String rowkey, String ColumnFamilys, String[] columns,
                                  String[] values) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(rowkey));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(ColumnFamilys), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
        }
        table.put(put);
        System.out.println("data inserted");
        table.close();
    }

    /**
     * 根据rowkey删除整行的所有列族、所有行、所有版本
     *
     * @param tablename
     * @param rowkey
     */
    public static void deleterow(String tablename, String rowkey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tablename));
        Delete delete = new Delete(Bytes.toBytes(rowkey));

        table.delete(delete);
        table.close();

        System.out.println("row" + rowkey + " is deleted");
    }

    /**
     * 删除某个row的指定列
     *
     * @param tablename
     * @param rowkey
     * @param columnfamily
     * @param column
     */
    public static void deletecol(String tablename, String rowkey, String columnfamily, String column)
            throws IOException {
        Table table = conn.getTable(TableName.valueOf(tablename));

        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.deleteColumn(Bytes.toBytes(columnfamily), Bytes.toBytes(column));

        table.delete(delete);
        table.close();

        System.out.println("row" + rowkey + " is deleted");
    }

    /**
     * 删除指定列族中所有列的时间戳等于指定时间戳的版本数据
     *
     * @param tablename
     * @param rowkey
     * @param columnfamily
     * @param timestamp
     */
    public static void deleteversion(String tablename, String rowkey, String columnfamily, Long timestamp)
            throws IOException {
        Table table = conn.getTable(TableName.valueOf(tablename));

        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.deleteFamilyVersion(Bytes.toBytes(columnfamily), timestamp);

        table.delete(delete);
        table.close();

        System.out.println("row" + rowkey + " is deleted");
    }

    /**
     * 删除指定列族,注意要先disable，修改完再enable表
     *
     * @param tablename,
     * @param columnfamily
     *
     */
    public static void deletefamily(String tablename, String columnfamily) throws IOException {
        Admin admin = conn.getAdmin();
        admin.disableTable(TableName.valueOf(tablename));
        HTableDescriptor table = admin.getTableDescriptor(TableName.valueOf(tablename));

        table.removeFamily(Bytes.toBytes(columnfamily));

        admin.modifyTable(TableName.valueOf(tablename), table);
        admin.enableTable(TableName.valueOf(tablename));
        System.out.println("columnfamily " + columnfamily + " is deleted");
        admin.close();
    }

    /**
     * drop表,注意要先disable表,否则会报错
     *
     * @param tablename
     */
    public static void droptable(String tablename) throws IOException {
        Admin admin = conn.getAdmin();
        admin.disableTable(TableName.valueOf(tablename));
        admin.deleteTable(TableName.valueOf(tablename));
        System.out.println("Table " + tablename + " is droped");
    }

    /**
     * 扫描全表
     *
     * @param tablename
     */
    public static void scantable(String tablename) throws IOException {
        Scan scan = new Scan();
        Table table = conn.getTable(TableName.valueOf(tablename));
        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            for (Cell cell : result.listCells()) {
                System.out.println(Bytes.toString(cell.getRow()) + "    " + "column=" + Bytes.toString(cell.getFamily())
                        + ":" + Bytes.toString(cell.getQualifier()) + ",timestamp=" + cell.getTimestamp() + ",value="
                        + Bytes.toString(cell.getValue()));
            }
        }
        rs.close();
    }

    /**
     * 根据rowkey对表进行scan
     *
     * @param tablename
     * @param rowkey
     *            scan 'student',{ROWPREFIXFILTER => '1'}
     */
    public static void scanrow(String tablename, String rowkey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        Table table = conn.getTable(TableName.valueOf(tablename));
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println(
                    rowkey + "    column=" + Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier())
                            + "," + "timestamp=" + kv.getTimestamp() + ",value=" + Bytes.toString(kv.getValue()));
        }
    }

    /**
     * 获取指定rowkey中，指定列的最新版本数据
     *
     * @param tablename
     * @param rowkey
     * @param columnfamily
     * @param column
     */
    public static void scanspecifycolumn(String tablename, String rowkey, String columnfamily, String column)
            throws IOException {
        Table table = conn.getTable(TableName.valueOf(tablename));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes(column));

        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println(
                    rowkey + "    column=" + Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier())
                            + "," + "timestamp=" + kv.getTimestamp() + ",value=" + Bytes.toString(kv.getValue()));
        }
    }

    /**
     * 获取行键指定的行中，指定时间戳的数据,
     *
     * @param tablename
     * @param rowkey
     * @param timestamp
     *            如果要获取指定时间戳范围的数据，可以使用get.setTimeRange方法
     */
    public static void scanspecifytimestamp(String tablename, String rowkey, Long timestamp) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setTimeStamp(timestamp);
        Table table = conn.getTable(TableName.valueOf(tablename));
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println(
                    rowkey + "    column=" + Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier())
                            + "," + "timestamp=" + kv.getTimestamp() + ",value=" + Bytes.toString(kv.getValue()));
        }
    }

    /**
     * 获取行键指定的行中，所有版本的数据
     * 能输出多版本数据的前提是当前列族能保存多版本数据，列族可以保存的数据版本数通过HColumnDescriptor的setMaxVersions(Int)方法设置。
     *
     * @param tablename
     * @param rowkey
     */
    public static void scanallversion(String tablename, String rowkey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setMaxVersions();
        Table table = conn.getTable(TableName.valueOf(tablename));
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println(
                    rowkey + "    column=" + Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier())
                            + "," + "timestamp=" + kv.getTimestamp() + ",value=" + Bytes.toString(kv.getValue()));
        }
    }

    /**
     * 使用过滤器，获取18-20岁之间的学生信息
     *
     * @param tablename
     * @param endage
     * @param startage
     * @throws IOException
     */
    public static void scanfilterage(String tablename, int startage, int endage) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tablename));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("information"),
                Bytes.toBytes("age"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startage));
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("information"),
                Bytes.toBytes("age"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(endage));
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);

        Scan scan = new Scan();
        scan.setFilter(filterList);

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            for (Cell cell : r.listCells()) {
                System.out.println(Bytes.toString(cell.getRow()) + "   Familiy:Quilifier : "
                        + Bytes.toString(cell.getFamily()) + ":" + Bytes.toString(cell.getQualifier()) + "   Value : "
                        + Bytes.toString(cell.getValue()) + "   Time : " + cell.getTimestamp());
            }
        }
        table.close();
    }

    public static void main(String[] args) throws IOException {
        String[] col1 = new String[] { "name", "age" };
        String[] val1 = new String[] { "xx", "18" };
        String[] col2 = new String[] { "chinese", "math" };
        String[] val2 = new String[] { "60", "70" };

        createtable("student", "imformation", "score");

//        insertdata("student", "1", "imformation", col1, val1);
//        insertdata("student", "1", "imformation", col2, val2);
//
//        deleterow("student", "1");
//        deletecol("student", "1", "imformation", "chinese");
//        deleteversion("student", "1", "imformation", 1533482642629L);
//        deletefamily("student", "imformation");
        droptable("student");
//
//        scantable("student");
//        scanrow("student", "1");
//        scanspecifycolumn("student", "1", "imformation", "chinese");
//        scanspecifytimestamp("student", "imformation", 1533482642629L);
//        scanallversion("student", "1");
//        scanfilterage("student", 18, 20);

    }

}
