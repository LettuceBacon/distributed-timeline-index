package xyz.mfj.dataDefiniation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcTable implements Table{
    private String tableName;
    private TypeDescription innerSchema;
    private TypeDescription schema;
    private HashMap<String, ApplicationPeriod> appPrds;
    // 表的mapreduce配置，主要包括：
    // 1.条带行数 OrcConf.STRIPE_ROW_COUNT OrcConf.ROWS_BETWEEN_CHECKS
    // 2.MR任务最大堆占用 OrcTable.MAXTASKMEM
    private Configuration conf;
    // 数据文件存储路径，名称为：表名+配置MR任务的时间
    private List<Path> storagePaths;
    
    public OrcTable(String tableName, 
        TypeDescription innerSchema, 
        TypeDescription schema,
        HashMap<String, ApplicationPeriod> appPrds,
        Configuration conf
    ) {
        this.tableName = tableName;
        this.innerSchema = innerSchema;
        this.schema = schema;
        this.appPrds = appPrds;
        this.conf = conf;
        this.storagePaths = new ArrayList<>();
    }

    
    @Override
    public boolean isColumnar() {
        return true;
    }

    @Override
    public boolean isTemporal() {
        return appPrds != null;
    }

    @Override
    public TypeDescription getTableSchema() {
        return this.schema;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public long getNumOfRow() {
        long numOfRow = 0L;
        try {
            for (Path p: storagePaths) {
                Reader fileReader = OrcFile.createReader(p, OrcFile.readerOptions(conf));
                numOfRow += fileReader.getNumberOfRows();
            }
            
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return numOfRow;
    }

    @Override
    public int getColumnSize() {
        return schema.getFieldNames().size();
    }
    
    @Override
    public Configuration getConf() {
        return conf;
    }
    
    @Override
    public List<Path> getStoragePath() {
        return storagePaths;
    }
    
    @Override
    public String getStorageFileType() {
        return "temporalorc";
    }
    
    @Override
    public TypeDescription getInnerSchema() {
        return innerSchema;
    }
    
    public ApplicationPeriod getAppPrd(String appPrdName) {
        return appPrds.get(appPrdName);
    }
    
    public HashMap<String, ApplicationPeriod> getAppPrds() {
        return appPrds;
    }
    
    public String schemaString() {
        StringBuilder sb = new StringBuilder();
        List<String> fieldNames = schema.getFieldNames();
        
        sb.append("----------------------------------------\n")
            .append(tableName)
            .append(":\n| ");
        for (String fieldName : fieldNames) {
            sb.append(fieldName).append(" | ");
        }
        sb.append("\n")
            .append("----------------------------------------");
        return sb.toString();
    }
}
