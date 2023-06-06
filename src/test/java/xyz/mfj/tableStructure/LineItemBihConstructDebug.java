package xyz.mfj.tableStructure;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import xyz.mfj.Library;
import xyz.mfj.DtlConf;
import xyz.mfj.dataDefiniation.OrcTable;
import xyz.mfj.dataDefiniation.OrcTableBuilder;
import xyz.mfj.dataManipulation.CsvInputMapper;
import xyz.mfj.dataManipulation.LoadDataExecutor;
import xyz.mfj.dataManipulation.LoadDataExecutorBuilder;
import xyz.mfj.dataManipulation.TemporalOrcOutputReducer;
import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;

public class LineItemBihConstructDebug {
    // 不同的运行模式，local是运行在mapreduce的local框架上，yarn是yarn框架上，mapreduce.framework.name
    private static final String RUNNING_MODEL = "local";
    // private static final String RUNNING_MODEL = "yarn";
    
    @Test
    public void constructLineItemBihDebug() 
        throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException 
    {
        Library lib = Library.getInstance();
        Configuration libConf = lib.getConf();
        // hadoop配置
        if (RUNNING_MODEL.equals("yarn")) {
            // 设置不生成_Success文件
            libConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", Boolean.FALSE.toString());
            libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/core-site.xml"));
            libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/hdfs-site.xml"));
            libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/yarn-site.xml"));
            libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/mapred-site.xml"));
        }
        
        // 表配置
        Configuration tblConf = new Configuration(libConf);
        OrcConf.COMPRESS.setString(tblConf, "NONE");
        // 256MB内存占用过大，不能用于stripeSize
        // 128MB
        // OrcConf.STRIPE_SIZE.setLong(tblConf, 134217728); 
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 2200000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 2200000); // 40172 
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 2240000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 2240000); // 40662 可能是因为减少了写入次数
        // 64MB
        tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        OrcConf.ROW_BATCH_SIZE.setInt(tblConf, EnhancedVectorizedRowBatch.INCREMENT_SIZE);
        OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1070000);
        OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1070000);
        // 32MB
        
        // 16MB
        // OrcConf.STRIPE_SIZE.setLong(tblConf, 8388608); 
        // OrcConf.ROW_BATCH_SIZE.setInt(tblConf, 1370);
        // // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 137216);
        // // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 137216);
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 137000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 137000);
        // tblConf.set("dfs.blocksize", "8388608");
        // tblConf.set("mapreduce.input.fileinputformat.split.minsize", "8388608");
        
        // 8MB
        // OrcConf.STRIPE_SIZE.setLong(tblConf, 8388608); 
        // OrcConf.ROW_BATCH_SIZE.setInt(tblConf, 1370);
        // // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 137216);
        // // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 137216);
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 137000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 137000);
        // tblConf.set("dfs.blocksize", "8388608");
        // tblConf.set("mapreduce.input.fileinputformat.split.minsize", "8388608");
        
        
        // 建表
        OrcTable lineitembih = new OrcTableBuilder()
            .createTable("lineitembih")
            .addColumn("orderKey", TypeDescription.createLong())
            .addColumn("partKey", TypeDescription.createLong())
            .addColumn("supplierKey", TypeDescription.createLong())
            .addColumn("lineNumber", TypeDescription.createInt())
            .addColumn("quantity", TypeDescription.createDecimal())
            .addColumn("extendedPrice", TypeDescription.createDecimal().withPrecision(10).withScale(2))
            .addColumn("discount", TypeDescription.createDecimal().withPrecision(10).withScale(2))
            .addColumn("tax", TypeDescription.createDecimal().withPrecision(10).withScale(2))
            .addColumn("returnFlag", TypeDescription.createChar().withMaxLength(3))
            .addColumn("status", TypeDescription.createChar().withMaxLength(3))
            .addColumn("shipDate", TypeDescription.createDate())
            .addColumn("commitDate", TypeDescription.createDate())
            .addColumn("receiptDate", TypeDescription.createDate())
            .addColumn("shipInstructions", TypeDescription.createChar().withMaxLength(25))
            .addColumn("shipMode", TypeDescription.createChar().withMaxLength(10))
            .addColumn("comment", TypeDescription.createVarchar().withMaxLength(44))
            .addColumn("activeTimeBegin", TypeDescription.createDate())
            .addColumn("activeTimeEnd", TypeDescription.createDate())
            .periodFor("activeTime", "activeTimeBegin", "activeTimeEnd")
            .withConf(tblConf)
            .build();
        lib.cacheTable(lineitembih);
            
        String inputPathStr = null;
        if (RUNNING_MODEL.equals("local")) {
            // inputPathStr = "/home/mfj/lineitembih/lineitembih-part-1-sf-0dot1.dat";
            inputPathStr = "/home/mfj/lineitembih/lineitembih-part-1-sf-1.dat";
        }
        if (RUNNING_MODEL.equals("yarn")) {
            inputPathStr = "/user/mfj/input/lineitembih-part-1-sf-1.dat";
        }
        
        LoadDataExecutor executor = new LoadDataExecutorBuilder()
            .intoTable("lineitembih")
            .fromPath(inputPathStr, "csv")
            .withProperties(CsvInputMapper.SEPARATORCHAR_CONF_NAME, "|")
            // .withParallelNum(2)
            .build();
            
        executor.execute();
        // executor.execute();
        // executor.execute();
    }
    
}
