package xyz.mfj;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import xyz.mfj.dataDefiniation.Table;

/**
 * 程序运行时常驻内存的库，用于保存时态表和timeline索引，以及hadoop配置
 */
public class Library {

    private final Configuration conf = new Configuration();
    // 表名与表的映射
    private HashMap<String, Table> tables;

    private static Library singleton = new Library();
    private Library() {}
    public static Library getInstance() {
        return singleton;
    }

    /**
     * 在内存中保存表
     * @param table
     */
    public void cacheTable(Table table) {
        if (tables == null) {
            tables = new HashMap<>();
        }
        tables.put(table.getTableName(), table);
    }

    /**
     * 根据表名获取表
     * @param tableName
     * @return
     */
    public Table getTableByName(String tableName) {
        return tables.get(tableName);
    }
    
    /**
     * 获取hadoop配置
     * @return
     */
    public Configuration getConf() {
        return conf;
    }

}
