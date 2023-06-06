package xyz.mfj.dataDefiniation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static xyz.mfj.enhanced.TypeDescriptionEnhance.*;

public class OrcTableBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(OrcTableBuilder.class);
    
    private String tableName;
    private TypeDescription innerSchema;
    private TypeDescription schema;
    private List<String> appPrdNames;
    private List<Integer> appPrdSIds;
    private List<Integer> appPrdEIds;
    private Configuration conf;
    
    public OrcTableBuilder() {}
    
    /**
     * CREATE TABLE 语句
     * @param tableName 表名
     * @return 
     */
    public OrcTableBuilder createTable(String tableName) {
        this.tableName = tableName;
        return this;
    }
    
    /**
     * 列定义语句
     * @param columnName 列名
     * @param type 列模式
     * @return 
     */
    public OrcTableBuilder addColumn(String columnName, TypeDescription type) {
        if (tableName == null) {
            LOG.error("Column defination must come after CREATE TABLE!");
            System.exit(1);
        }
        if (!type.getCategory().isPrimitive()) {
            LOG.error("Table doesn't support compond types!");
            System.exit(1);
        }
        if (this.innerSchema == null) {
            this.innerSchema = TypeDescription.createStruct();
            this.innerSchema.getId();
            this.schema = TypeDescription.createStruct();
            this.schema.getId();
        }
        structAddField(this.innerSchema, columnName, type);
        structAddField(this.schema, columnName, type);
        return this;
    }
    
    /**
     * PERIOD FOR 语句。用于定义有效时间和为有效时间构造索引。
     * @param appPeriodName 有效时间名称
     * @param periodStartName 有效时间起始时间名称
     * @param periodEndName 有效时间终止时间名称
     * @return 
     */
    public OrcTableBuilder periodFor(String appPeriodName, 
        String periodStartName, String periodEndName)
    {
        if (appPrdNames == null) {
            appPrdNames = new ArrayList<>();
            appPrdSIds = new ArrayList<>();
            appPrdEIds = new ArrayList<>();
        }
        appPrdNames.add(appPeriodName);
        TypeDescription startType = null;
        TypeDescription endType = null;
        try {
            startType = innerSchema.findSubtype(periodStartName);
            endType = innerSchema.findSubtype(periodEndName);
        } catch (IllegalArgumentException e) {
            LOG.error("Period start time or period end time was not founded in table schema!\n {}", e);
            System.exit(1);
        }
        if (!startType.getCategory().equals(TypeDescription.Category.DATE)
            && !startType.getCategory().equals(TypeDescription.Category.TIMESTAMP)
        ) {
            LOG.error("PERIOD FOR clause can only apply on time type!");
            System.exit(1);
        }
        if (!startType.getCategory().equals(endType.getCategory())) {
            LOG.error("Period start `{}` has different type of Period end `{}`", 
                periodStartName, periodEndName);
            System.exit(1);
        }
        appPrdSIds.add(startType.getId() - 1);
        appPrdEIds.add(endType.getId() - 1);
        return this;
    }
    
    /**
     * Set custom configuration for this table
     * @param conf configuration
     * @return builder
     */
    public OrcTableBuilder withConf(Configuration conf) {
        this.conf = conf;
        return this;
    }
    
    public OrcTable build() {
        if (tableName == null 
            || innerSchema == null 
            || innerSchema.getCategory() != Category.STRUCT
            || schema == null
        ) {
            LOG.error("CREATE TABLE lacks key clause. Need CREATE TABLE and column definations!");
            System.exit(1);
        }
        if (conf == null) {
            conf = new Configuration();
        }
        HashMap<String, ApplicationPeriod> appPrds = null;
        if (appPrdNames != null) {
            appPrds = new HashMap<>();
            // 将索引列加在内模式的最后，索引列是rowId和flag的组合
            for (int i = 0; i < appPrdNames.size(); ++i) {
                String appPrdName = appPrdNames.get(i);
                // 索引前半部分的内模式
                TypeDescription rf0type = TypeDescription.createInt();
                structAddField(innerSchema, appPrdName + TimelineIndex.RF0, rf0type);
                // 索引后半部分的内模式
                TypeDescription rf1type = TypeDescription.createInt();
                structAddField(innerSchema, appPrdName + TimelineIndex.RF1, rf1type);
                appPrds.put(
                    appPrdName, 
                    new ApplicationPeriod(appPrdName, 
                        appPrdSIds.get(i), 
                        appPrdEIds.get(i), 
                        rf0type.getId() - 1, 
                        rf1type.getId() - 1, 
                        new TimelineIndex())
                );
            }
        }
        return new OrcTable(tableName, innerSchema, schema, appPrds, conf);
    }
}