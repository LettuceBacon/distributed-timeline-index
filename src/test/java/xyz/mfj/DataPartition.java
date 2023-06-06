package xyz.mfj;

import java.io.IOException;
import java.time.LocalDate;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.Text;
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
import xyz.mfj.dataQuery.DtlExpression;
import xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecuter;
import xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecutorBuilder;
import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;

public class DataPartition {
    
    @Test
    public void loadData() throws ClassNotFoundException, IOException, InterruptedException {
        Library lib = Library.getInstance();
        Configuration libConf = lib.getConf();
        // hadoop配置
        libConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", Boolean.FALSE.toString());
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/core-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/hdfs-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/yarn-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/mapred-site.xml"));
        
        // 表配置
        Configuration tblConf = new Configuration(libConf);
        OrcConf.COMPRESS.setString(tblConf, "NONE");
        OrcConf.ROW_BATCH_SIZE.setInt(tblConf, EnhancedVectorizedRowBatch.INCREMENT_SIZE);

        // 128MB
        tblConf.set(DtlConf.MAX_TASK_MEM, "2304M");
        OrcConf.STRIPE_SIZE.setLong(tblConf, 134217728); 
        OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 2185000);
        OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 2185000);
        // Construct table and Timeline index cost 74741 ms! 1894.65MB 1861.81MB
        // Construct table and Timeline index cost 78652 ms! 1982.23MB 1927.63MB
        // Construct table and Timeline index cost 74269 ms! 1960.03MB 1848.78MB
        
        // 64MB
        // tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        // OrcConf.STRIPE_SIZE.setLong(tblConf, 67108864); 
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1075000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1075000);
        // Construct table and Timeline index cost 113696 ms! 1077.59MB 1253.61MB
        // Construct table and Timeline index cost 69719 ms! 1145.26MB 1139.65MB
        // Construct table and Timeline index cost 69539 ms! 1115.71MB 1213.02MB

        // 32MB
        // tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        // OrcConf.STRIPE_SIZE.setLong(tblConf, 33554432); 
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 535000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 535000); 
        // Construct table and Timeline index cost 63252 ms! 873.54MB 919.72MB
        // Construct table and Timeline index cost 65662 ms! 868.81MB 913.43MB
        // Construct table and Timeline index cost 62838 ms! 807.50MB 785.58MB
        
        // 16MB
        // tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        // OrcConf.STRIPE_SIZE.setLong(tblConf, 16777216); 
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 265000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 265000);
        // Construct table and Timeline index cost 58449 ms! 727.93MB 808.23MB
        // Construct table and Timeline index cost 60663 ms! 813.55MB 778.20MB
        // Construct table and Timeline index cost 58998 ms! 802.63MB 549.82MB
        
        
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
        inputPathStr = "/user/mfj/input/lineitembih-part-1-sf-1.dat";
        
        LoadDataExecutor executor = new LoadDataExecutorBuilder()
            .intoTable("lineitembih")
            .fromPath(inputPathStr, "csv")
            .withProperties(CsvInputMapper.SEPARATORCHAR_CONF_NAME, "|")
            .withParallelNum(2)
            .build();
            
        executor.execute();
        executor.execute();
        executor.execute();
    }
    
    @Test
    public void timeTravel() throws ClassNotFoundException, IOException, InterruptedException {
        Library lib = Library.getInstance();
        Configuration libConf = lib.getConf();
        // hadoop配置
        libConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", Boolean.FALSE.toString());
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/core-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/hdfs-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/yarn-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/mapred-site.xml"));
        
        // 表配置
        Configuration tblConf = new Configuration(libConf);
        OrcConf.COMPRESS.setString(tblConf, "NONE");
        OrcConf.ROW_BATCH_SIZE.setInt(tblConf, EnhancedVectorizedRowBatch.INCREMENT_SIZE);

        // 128MB
        // tblConf.set(DtlConf.MAX_TASK_MEM, "2304M");
        // OrcConf.STRIPE_SIZE.setLong(tblConf, 134217728); 
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 2185000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 2185000);
        // Time taken 25545 ms 
        // Time taken 24761 ms
        // Time taken 24682 ms
        
        // 64MB
        // tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        // OrcConf.STRIPE_SIZE.setLong(tblConf, 67108864); 
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1075000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1075000);
        // Time taken 22318 ms
        // Time taken 22659 ms
        // Time taken 22509 ms

        // 32MB
        // tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        // OrcConf.STRIPE_SIZE.setLong(tblConf, 33554432); 
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 535000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 535000); 
        // Time taken 20614 ms
        // Time taken 21883 ms
        // Time taken 20636 ms
        
        // 16MB
        tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        OrcConf.STRIPE_SIZE.setLong(tblConf, 16777216); 
        OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 265000);
        OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 265000);
        // Time taken 20711 ms
        // Time taken 19458 ms
        // Time taken 20218 ms
        
        // 猜测是条带小，mapper里遍历快，经过shuffle有序后，归并也是只是把所有数据再读一遍，
        // 什么情况下块越小，归并越慢？mapper太少了，起码得几千个mapper才能出现让归并变慢，reducer从几千个mapper里获取数据然后归并排序会很慢
        // ITISS说20K到200K条一个分区最快
        // 就是说我得让inputsplit变小，条带也变小，这样可以生成几千个mapper，只要启动的mapper的开销以及将各mapper生成的数据归并的开销够大就能证明条带不是越小越好
        // TODO：设定inputsplit和条带大小，条带行数根据条带大小自动适配（平均值法），然后从一个64M大小递减到1MB(大概三百多个条带和对应的mapper)
        // TODO：首先设定条带大小，然后loadData，然后设定inputsplit，然后执行三次query并记录时间，接着设定下一级条带大小，循环，最后结束，
        // TODO：既需要单独设定inputsplit，又需要单独设定条带行数，因此要让各操作的builder提供withProperty方法来灵活地设置这些东西，同时在实验的合适位置编排这些操作
        // TODO：inputsplit只有查询需要设置，根据条带大小设置
        // TODO：条带行数需要由表设置，自动设置在loadData里，手动设置在表构造中
        // TODO：压缩和行组行数均由表设置，手动在表构造时配置
        // 因为mapper同时只有6个在工作，因此mapper并不是所有的同时启动，split越多越小，mapper的启动成本越大
        // 目的是查看分区大小对于部分查询和归并的影响，理论上讲，split越多越小，部分查询消耗时间更少，但归并算法的耗时更长
        // TODO：应用不同的分区大小，对所有可以测量的数据进行自动测试，然后分析为什么并挑选一些可以说的
        // TODO：另外参照其他论文看看有什么实验可以设置
        // TODO：在保证所有hadoop组件条件一致的情况下进行实验，那么有哪些需要保证一致的？主要是那些hadoop自动调节的工作流程
        
        
        
        
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
        inputPathStr = "/user/mfj/input/lineitembih-part-1-sf-1.dat";
        
        LoadDataExecutor ldExecutor = new LoadDataExecutorBuilder()
            .intoTable("lineitembih")
            .fromPath(inputPathStr, "csv")
            .withProperties(CsvInputMapper.SEPARATORCHAR_CONF_NAME, "|")
            .withParallelNum(2)
            .build();
            
        ldExecutor.execute();
        
        TimeTravelQueryExecuter executor = new TimeTravelQueryExecutorBuilder()
            .fromTable("lineitembih")
            .periodContainsTime("activeTime", LocalDate.parse("1996-02-12"))
            .withWhereClause(
                new DtlExpression("#lineNumber < 4", 
                    new int[]{3}, 
                    new String[]{"lineNumber"}),
                new Consumer<SearchArgument.Builder>() {

                    @Override
                    public void accept(SearchArgument.Builder builder) {
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
        executor.execute();
        executor.execute();
    }
    
    @Test
    public void temporalGrouping() {
        
    }
}
