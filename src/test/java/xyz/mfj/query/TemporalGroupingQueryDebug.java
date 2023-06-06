package xyz.mfj.query;

import java.io.IOException;
import java.time.LocalDate;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import xyz.mfj.Library;
import xyz.mfj.DtlConf;
import xyz.mfj.dataDefiniation.OrcTable;
import xyz.mfj.dataDefiniation.OrcTableBuilder;
import xyz.mfj.dataManipulation.CsvInputMapper;
import xyz.mfj.dataManipulation.LoadDataExecutor;
import xyz.mfj.dataManipulation.LoadDataExecutorBuilder;
import xyz.mfj.dataQuery.DtlExpression;
import xyz.mfj.dataQuery.temporalGroupingQuery.TemporalGroupingQueryExecutor;
import xyz.mfj.dataQuery.temporalGroupingQuery.TemporalGroupingQueryExecutorBuilder;
import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;

public class TemporalGroupingQueryDebug {
    // 不同的运行模式，local是运行在mapreduce的local框架上，yarn是yarn框架上，mapreduce.framework.name
    // private static final String RUNNING_MODEL = "local";
    private static final String RUNNING_MODEL = "yarn";
    
    @BeforeEach
    public void preparation() throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
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
        // tblConf.set("mapreduce.input.fileinputformat.split.minsize", "67108864");
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1100000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1100000);
        OrcConf.ROW_BATCH_SIZE.setInt(tblConf, EnhancedVectorizedRowBatch.INCREMENT_SIZE);
        tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1070000);
        OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1070000);
        // 8MB
        // OrcConf.STRIPE_SIZE.setLong(tblConf, 8388608); 
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 137216);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 137216);
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
            .withParallelNum(2)
            .build();
            
        executor.execute();
    }
    
    // -- 2649544 | 915.00 | 0.10 |
    // -- 2650461 | 915.00 | 0.10 |
    // -- 2650072 | 915.00 | 0.10 |
    // -- 2649626 | 915.00 | 0.10 |
    // -- 2649755 | 915.00 | 0.10 |
    // -- 2652653 | 915.00 | 0.10 |
    // -- 2650400 | 915.00 | 0.10 |
    // -- 2649908 | 915.00 | 0.10 |
    // -- 2647030 | 915.00 | 0.10 |
    // -- 2651442 | 915.00 | 0.10 |
    // -- 2651482 | 915.00 | 0.10 |
    // -- 2651968 | 915.00 | 0.10 |
    // -- 2650367 | 915.00 | 0.10 |
    // -- 2652421 | 915.00 | 0.10 |
    // -- 2655053 | 915.00 | 0.10 |
    // -- 2655425 | 915.00 | 0.10 |
    // -- 2657354 | 915.00 | 0.10 |
    // -- 2656634 | 915.00 | 0.10 |
    // -- 2658663 | 915.00 | 0.10 |
    // -- 2659143 | 915.00 | 0.10 |
    // -- 2661881 | 915.00 | 0.10 |
    // -- 2661321 | 915.00 | 0.10 |
    // -- 2662756 | 915.00 | 0.10 |
    // -- 2663262 | 915.00 | 0.10 |
    // -- 2664169 | 915.00 | 0.10 |
    // -- 2665533 | 915.00 | 0.10 |
    // -- 2669613 | 915.00 | 0.10 |
    // -- 2672709 | 915.00 | 0.10 |
    // -- 2671183 | 916.00 | 0.10 |
    @Test
    public void aggregateQueryDebug() 
        throws IllegalArgumentException, ClassNotFoundException, IOException, InterruptedException 
    {
        TemporalGroupingQueryExecutor executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("lineitembih")
            .groupByPeriodOverlaps("activeTime", LocalDate.parse("1996-02-12"), LocalDate.parse("1996-03-12"))
            .withWhereClause(
                new DtlExpression("#returnFlag == 'N'", 
                    new int[]{8}, 
                    new String[]{"returnFlag"}), 
                new Consumer<SearchArgument.Builder>() {

                    @Override
                    public void accept(Builder builder) {
                        builder.equals("returnFlag", PredicateLeaf.Type.STRING, "N");
                    }
                    
                }
            )
            .withSelectClause(
                new DtlExpression("#quantity",
                    new int[]{4}, 
                    new String[]{"quantity"}), 
                new Text("sum(quantity)"), 
                new Text("decimal(10,2)"), 
                new Text("sum")
            )
            .withSelectClause(
                new DtlExpression("#extendedPrice",
                    new int[]{5}, 
                    new String[]{"extendedPrice"}), 
                new Text("min(extendedPrice)"), 
                new Text("decimal(10,2)"), 
                new Text("min")
            )
            .withSelectClause(
                new DtlExpression("#discount",
                    new int[]{6}, 
                    new String[]{"discount"}), 
                new Text("max(discount)"), 
                new Text("decimal(10,2)"), 
                new Text("max")
            )
            .build();
        executor.execute();
        
        // Time taken 6134 ms
        // Time taken 5163 ms
    }
    
}
