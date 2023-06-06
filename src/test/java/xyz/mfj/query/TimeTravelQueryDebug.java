package xyz.mfj.query;

import java.io.IOException;
import java.time.LocalDate;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
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
import xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecuter;
import xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecutorBuilder;
import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;

public class TimeTravelQueryDebug {
    // 不同的运行模式，mapreduce.framework.name
    // local是运行在mapreduce的local框架上，yarn是yarn框架上
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
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 2200000);
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 2240000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 2240000);
        // 64MB
        OrcConf.ROW_BATCH_SIZE.setInt(tblConf, EnhancedVectorizedRowBatch.INCREMENT_SIZE);
        tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1065000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1065000); // 3090 ms
        OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1070000);
        OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1070000); // 3116 ms
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1075000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1075000); // 3077 ms
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1080000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1080000); // 3094 ms
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1085000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1085000); // 3100 ms 最大行数
        // 8MB
        // OrcConf.STRIPE_SIZE.setLong(tblConf, 8388608); 
        // // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 137216);
        // // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 137216);
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 137000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 137000);
        // tblConf.set("dfs.blocksize", "8388608");
        // tblConf.set("mapreduce.input.fileinputformat.split.minsize", "8388608");
        
        // no padding
        // [main] INFO xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecuter - Time taken 20768 ms
        // [main] INFO xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecuter - Time taken 21926 ms
        // [main] INFO xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecuter - Time taken 21651 ms

        // padding
        // File length: 368805233 bytes
        // Padding length: 6169890 bytes
        // Padding ratio: 1.67%
        // [main] INFO xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecuter - Time taken 22724 ms
        // [main] INFO xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecuter - Time taken 22258 ms
        // [main] INFO xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecuter - Time taken 22041 ms
        
        
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
    
// sql query statement
// # HIVE:
// select L_ORDERKEY, L_PARTKEY, L_SUPPKEY, l_active_time_begin, l_active_time_end
// from LINEITEM_orc
// where L_LINENUMBER = 6 and L_DISCOUNT > 0.03 and l_active_time_begin <= '1996-02-12' and l_active_time_end > '1996-02-12'
    @Test
    public void timeTravelQueryDebug() 
        throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException 
    {
        TimeTravelQueryExecutorBuilder builder = new TimeTravelQueryExecutorBuilder();
        builder.fromTable("lineitembih");
        builder.periodContainsTime("activeTime", LocalDate.parse("1996-02-12"));
        builder.withWhereClause(
            new DtlExpression("#lineNumber == 6 && #discount > 0.03", 
                new int[]{3, 6}, 
                new String[]{"lineNumber", "discount"}), 
            new Consumer<SearchArgument.Builder>() {

                @Override
                public void accept(Builder builder) {
                    builder.startAnd()
                        .equals("lineNumber", PredicateLeaf.Type.LONG, Long.valueOf(6))
                        .startNot()
                        .lessThanEquals("discount", 
                            PredicateLeaf.Type.DECIMAL, 
                            new HiveDecimalWritable("0.03"))
                        .end() // end not
                        .end(); // end and
                }
                
            }
        );
        builder.withSelectClause(
            new DtlExpression("#orderKey",
                new int[]{0}, 
                new String[]{"orderKey"}), 
            new Text("orderKey"), 
            new Text("bigint"), 
            null
        );
        builder.withSelectClause(
            new DtlExpression("#partKey",
                new int[]{1}, 
                new String[]{"partKey"}), 
            new Text("partKey"), 
            new Text("bigint"), 
            null
        );
        builder.withSelectClause(
            new DtlExpression("#supplierKey",
                new int[]{2}, 
                new String[]{"supplierKey"}), 
            new Text("supplierKey"), 
            new Text("bigint"), 
            null
        );
        builder.withSelectClause(
            new DtlExpression("#activeTimeBegin",
                new int[]{16}, 
                new String[]{"activeTimeBegin"}), 
            new Text("activeTimeBegin"), 
            new Text("date"), 
            null
        );
        builder.withSelectClause(
            new DtlExpression("#activeTimeEnd",
                new int[]{17}, 
                new String[]{"activeTimeEnd"}), 
            new Text("activeTimeEnd"), 
            new Text("date"), 
            null
        );
        // builder.withProperty("mapreduce.input.fileinputformat.split.maxsize", "33554432");
        
        TimeTravelQueryExecuter executer = builder.build();
        executer.execute();
    }
    
// sql query statement
// # HIVE:
// select
//     sum(L_EXTENDEDPRICE)
// from
//     lineitem_orc
// where
//     L_LINENUMBER < 4
//     and l_active_time_begin <= '1996-02-12'
//     and l_active_time_end > '1996-02-12'
// 245564552.25
    @Test
    public void TimeTravelQueryWithSumAggDebug() throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        TimeTravelQueryExecuter executor = new TimeTravelQueryExecutorBuilder()
            .fromTable("lineitembih")
            .periodContainsTime("activeTime", LocalDate.parse("1996-02-12"))
            .withWhereClause(
                new DtlExpression("#lineNumber < 4", 
                    new int[]{3}, 
                    new String[]{"lineNumber"}),
                new Consumer<SearchArgument.Builder>() {

                    @Override
                    public void accept(Builder builder) {
                        builder.lessThan("lineNumber", PredicateLeaf.Type.LONG, Long.valueOf(4));
                    }
                    
                }
            )
            .withSelectClause(
                new DtlExpression("#extendedPrice", 
                    new int[]{5}, 
                    new String[]{"extendedPrice"}),
                new Text("sum(extendedPrice)"),
                new Text("decimal(12,2)"),
                new Text("sum")
            )
            .build();

        executor.execute();
    }
    
// sql query statement
// # HIVE:
// select
//     max(L_EXTENDEDPRICE)
// from
//     lineitem_orc
// where
//     L_LINENUMBER < 6
//     and l_active_time_begin <= '1996-02-12'
//     and l_active_time_end > '1996-02-12'
// 94949.50
    @Test
    public void TimeTravelQueryWithMaxAggDebug() throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        TimeTravelQueryExecuter executor = new TimeTravelQueryExecutorBuilder()
            .fromTable("lineitembih")
            .periodContainsTime("activeTime", LocalDate.parse("1996-02-12"))
            .withWhereClause(
                new DtlExpression("#lineNumber < 6", 
                    new int[]{3}, 
                    new String[]{"lineNumber"}),
                new Consumer<SearchArgument.Builder>() {

                    @Override
                    public void accept(Builder builder) {
                        builder.lessThan("lineNumber", PredicateLeaf.Type.LONG, Long.valueOf(6));
                    }
                    
                }
            )
            .withSelectClause(
                new DtlExpression("#extendedPrice", 
                    new int[]{5}, 
                    new String[]{"extendedPrice"}),
                new Text("max(extendedPrice)"),
                new Text("decimal(12,2)"),
                new Text("max")
            )
            .build();

        executor.execute();
    }

}
