package xyz.mfj;

import java.io.IOException;
import java.sql.Timestamp;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.Library;
import xyz.mfj.DtlConf;
import xyz.mfj.dataDefiniation.OrcTable;
import xyz.mfj.dataDefiniation.OrcTableBuilder;
import xyz.mfj.dataManipulation.CsvInputMapper;
import xyz.mfj.dataManipulation.LoadDataExecutor;
import xyz.mfj.dataManipulation.LoadDataExecutorBuilder;
import xyz.mfj.dataManipulation.TemporalOrcOutputReducer;
import xyz.mfj.dataQuery.DtlExpression;
import xyz.mfj.dataQuery.temporalGroupingQuery.TemporalGroupingQueryExecutor;
import xyz.mfj.dataQuery.temporalGroupingQuery.TemporalGroupingQueryExecutorBuilder;
import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;

public class TemporalGroupingQuery {
    // A型查询，测量基本的运行时间和内存占用
    // B型查询，验证有索引的能够少处理一些数据，速度更快
    // C型查询，验证有索引的在面对选择型聚合函数时更省内存
    // A1：查询时间段是中等跨度，位于数据集整个时间段的中间50%的部分，只包含一个select表达式，表达式中只有一个累积型agg
    // A2：查询时间段是中等跨度，位于数据集整个时间段的中间50%的部分，只包含一个select表达式，表达式中只有一个选择型agg
    // B1：查询时间段是短跨度，位于数据集整个时间段的前25%的部分，只包含一个累积型聚合函数的select表达式
    // B2：查询时间段是短跨度，位于数据集整个时间段的后25%的部分，只包含一个累积型聚合函数的select表达式
    // B3：查询时间段是中等跨度，位于数据集整个时间段的前25%的部分，只包含一个累积型聚合函数的select表达式
    // B4：查询时间段是中等跨度，位于数据集整个时间段的后25%的部分，只包含一个累积型聚合函数的select表达式
    // B5：查询数据集包含的整个时间段，只包含一个累积型聚合函数的select表达式
    // C1：查询时间段是短跨度，位于数据集整个时间段的中间50%的部分，包括多个select表达式，每个表达式都包括一个选择型聚合函数
    
    @Test
    public void tpcbihTemporalGroupingQuery() 
        throws IllegalArgumentException, ClassNotFoundException, IOException, InterruptedException 
    {
        Library lib = Library.getInstance();
        Configuration libConf = lib.getConf();
        
        libConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", Boolean.FALSE.toString());
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/core-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/hdfs-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/yarn-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/mapred-site.xml"));
        
        // tpc-bih sf1 sf10 lineitem
        // 表配置
        Configuration tblConf = new Configuration(libConf);
        OrcConf.COMPRESS.setString(tblConf, "NONE");
        // 64MB
        tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        OrcConf.ROW_BATCH_SIZE.setInt(tblConf, EnhancedVectorizedRowBatch.INCREMENT_SIZE);
        OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1070000);
        OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1070000);

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
            
        // String inputPathStr = "/user/mfj/input/lineitembih-part-1-sf-1.dat";
        String inputPathStr = "/user/mfj/input/lineitembih-part-1-sf-10.dat";
        
        LoadDataExecutor executor = new LoadDataExecutorBuilder()
            .intoTable("lineitembih")
            .fromPath(inputPathStr, "csv")
            .withProperties(CsvInputMapper.SEPARATORCHAR_CONF_NAME, "|")
            .withParallelNum(2)
            .build();
            
        executor.execute();
        
        // 查询
        TemporalGroupingQueryExecutor a1executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("lineitembih")
            .groupByPeriodOverlaps("activeTime", LocalDate.parse("1995-02-12"), LocalDate.parse("1995-03-10"))
            .withSelectClause(
                new DtlExpression("#quantity",
                    new int[]{4}, 
                    new String[]{"quantity"}), 
                new Text("sum(quantity)"), 
                new Text("decimal(12,2)"), 
                new Text("sum")
            )
            .build();
        a1executor.execute();
        a1executor.execute();
        a1executor.execute();
        
        TemporalGroupingQueryExecutor a2executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("lineitembih")
            .groupByPeriodOverlaps("activeTime", LocalDate.parse("1995-02-12"), LocalDate.parse("1995-03-10"))
            .withSelectClause(
                new DtlExpression("#extendedPrice",
                    new int[]{5}, 
                    new String[]{"extendedPrice"}), 
                new Text("min(extendedPrice)"), 
                new Text("decimal(10,2)"), 
                new Text("min")
            )
            .build();
        a2executor.execute();
        a2executor.execute();
        a2executor.execute();
        
        TemporalGroupingQueryExecutor b1executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("lineitembih")
            .groupByPeriodOverlaps("activeTime", LocalDate.parse("1992-01-12"), LocalDate.parse("1992-01-15"))
            .withSelectClause(
                new DtlExpression("#quantity",
                    new int[]{4}, 
                    new String[]{"quantity"}), 
                new Text("sum(quantity)"), 
                new Text("decimal(12,2)"), 
                new Text("sum")
            )
            .build();
        b1executor.execute();
        b1executor.execute();
        b1executor.execute();
        
        TemporalGroupingQueryExecutor b2executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("lineitembih")
            .groupByPeriodOverlaps("activeTime", LocalDate.parse("1998-11-12"), LocalDate.parse("1998-11-15"))
            .withSelectClause(
                new DtlExpression("#quantity",
                    new int[]{4}, 
                    new String[]{"quantity"}), 
                new Text("sum(quantity)"), 
                new Text("decimal(12,2)"), 
                new Text("sum")
            )
            .build();
        b2executor.execute();
        b2executor.execute();
        b2executor.execute();
        
        TemporalGroupingQueryExecutor b3executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("lineitembih")
            .groupByPeriodOverlaps("activeTime", LocalDate.parse("1992-01-12"), LocalDate.parse("1992-02-07"))
            .withSelectClause(
                new DtlExpression("#quantity",
                    new int[]{4}, 
                    new String[]{"quantity"}), 
                new Text("sum(quantity)"), 
                new Text("decimal(12,2)"), 
                new Text("sum")
            )
            .build();
        b3executor.execute();
        b3executor.execute();
        b3executor.execute();
        
        TemporalGroupingQueryExecutor b4executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("lineitembih")
            .groupByPeriodOverlaps("activeTime", LocalDate.parse("1998-11-12"), LocalDate.parse("1998-12-08"))
            .withSelectClause(
                new DtlExpression("#quantity",
                    new int[]{4}, 
                    new String[]{"quantity"}), 
                new Text("sum(quantity)"), 
                new Text("decimal(12,2)"), 
                new Text("sum")
            )
            .build();
        b4executor.execute();
        b4executor.execute();
        b4executor.execute();
        
        TemporalGroupingQueryExecutor b5executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("lineitembih")
            .groupByPeriodOverlaps("activeTime", LocalDate.parse("1992-01-01"), LocalDate.parse("1998-12-31"))
            .withSelectClause(
                new DtlExpression("#quantity",
                    new int[]{4}, 
                    new String[]{"quantity"}), 
                new Text("sum(quantity)"), 
                new Text("decimal(12,2)"), 
                new Text("sum")
            )
            .build();
        b5executor.execute();
        b5executor.execute();
        b5executor.execute();
        
        TemporalGroupingQueryExecutor c1executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("lineitembih")
            .groupByPeriodOverlaps("activeTime", LocalDate.parse("1995-02-12"), LocalDate.parse("1995-02-15"))
            .withSelectClause(
                new DtlExpression("#quantity",
                    new int[]{4}, 
                    new String[]{"quantity"}), 
                new Text("max(quantity)"), 
                new Text("decimal(10,2)"), 
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#extendedPrice",
                    new int[]{5}, 
                    new String[]{"extendedPrice"}), 
                new Text("max(extendedPrice)"), 
                new Text("decimal(10,2)"), 
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#discount",
                    new int[]{6}, 
                    new String[]{"discount"}), 
                new Text("max(discount)"), 
                new Text("decimal(10,2)"), 
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#extendedPrice * (1 - #discount)",
                    new int[]{5, 6}, 
                    new String[]{"extendedPrice", "discount"}), 
                new Text("max_discounted_price)"), 
                new Text("decimal(14,6)"), 
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#extendedPrice * (1 - #discount) * (1 + #tax)",
                    new int[]{5, 6, 7}, 
                    new String[]{"extendedPrice", "discount", "tax"}), 
                new Text("max_charge"), 
                new Text("decimal(14,6)"), 
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#shipDate",
                    new int[]{10}, 
                    new String[]{"shipDate"}), 
                new Text("max_shipDate"), 
                new Text("date"), 
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#commitDate",
                    new int[]{11}, 
                    new String[]{"commitDate"}), 
                new Text("max_commitDate"), 
                new Text("date"), 
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#receiptDate",
                    new int[]{12}, 
                    new String[]{"receiptDate"}), 
                new Text("max_receiptDate"), 
                new Text("date"), 
                new Text("max")
            )
            .build();
        c1executor.execute();
        c1executor.execute();
        c1executor.execute();
    }
    
    @Test
    public void yttr2022TemporalGroupingQuery() 
        throws IllegalArgumentException, ClassNotFoundException, IOException, InterruptedException 
    {
        Library lib = Library.getInstance();
        Configuration libConf = lib.getConf();
        
        libConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", Boolean.FALSE.toString());
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/core-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/hdfs-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/yarn-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/mapred-site.xml"));
        
        // 表配置
        Configuration tblConf = new Configuration(libConf);
        OrcConf.COMPRESS.setString(tblConf, "NONE");
        // 256MB内存占用过大，不能用于stripeSize
        // 128MB
        // 64MB
        tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        OrcConf.ROW_BATCH_SIZE.setInt(tblConf, EnhancedVectorizedRowBatch.INCREMENT_SIZE);
        OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 580000);
        OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 580000);
        // 32MB
        // 16MB
        // 8MB
        
        // 建表
        OrcTable YTTP2022 = new OrcTableBuilder()
            .createTable("YTTP2022")
            .addColumn("VendorID", TypeDescription.createLong())
            .addColumn("tpep_pickup_datetime", TypeDescription.createTimestamp())
            .addColumn("tpep_dropoff_datetime", TypeDescription.createTimestamp())
            .addColumn("passenger_count", TypeDescription.createDouble())
            .addColumn("trip_distance", TypeDescription.createDouble())
            .addColumn("RatecodeID", TypeDescription.createDouble())
            .addColumn("store_and_fwd_flag", TypeDescription.createString())
            .addColumn("PULocationID", TypeDescription.createLong())
            .addColumn("DOLocationID", TypeDescription.createLong())
            .addColumn("payment_type", TypeDescription.createLong())
            .addColumn("fare_amount", TypeDescription.createDouble())
            .addColumn("extra", TypeDescription.createDouble())
            .addColumn("mta_tax", TypeDescription.createDouble())
            .addColumn("tip_amount", TypeDescription.createDouble())
            .addColumn("tolls_amount", TypeDescription.createDouble())
            .addColumn("improvement_surcharge", TypeDescription.createDouble())
            .addColumn("total_amount", TypeDescription.createDouble())
            .addColumn("congestion_surcharge", TypeDescription.createDouble())
            .addColumn("airport_fee", TypeDescription.createDouble())
            .periodFor("tpep", "tpep_pickup_datetime", "tpep_dropoff_datetime")
            .withConf(tblConf)
            .build();
        lib.cacheTable(YTTP2022);
            
        String inputPathStr = "/user/mfj/input/yellow_tripdata_2022";
        
        LoadDataExecutor executor = new LoadDataExecutorBuilder()
            .intoTable("YTTP2022")
            .fromPath(inputPathStr, "csv")
            .withParallelNum(2)
            .build();
            
        executor.execute();
        
        // 查询
        TemporalGroupingQueryExecutor a1executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("YTTP2022")
            .groupByPeriodOverlaps("tpep", 
                Timestamp.valueOf("2022-06-24 00:00:00"), 
                Timestamp.valueOf("2022-06-27 23:36:00"))
            .withSelectClause(
                new DtlExpression("#total_amount",
                    new int[]{16}, 
                    new String[]{"total_amount"}), 
                new Text("sum(total_amount)"), 
                new Text("double"), 
                new Text("sum")
            )
            .build();
        a1executor.execute();
        a1executor.execute();
        a1executor.execute();
        
        TemporalGroupingQueryExecutor a2executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("YTTP2022")
            .groupByPeriodOverlaps("tpep", 
                Timestamp.valueOf("2022-06-24 00:00:00"), 
                Timestamp.valueOf("2022-06-27 23:36:00"))
            .withSelectClause(
                new DtlExpression("#fare_amount",
                    new int[]{10}, 
                    new String[]{"fare_amount"}), 
                new Text("min(fare_amount)"), 
                new Text("double"), 
                new Text("min")
            )
            .build();
        a2executor.execute();
        a2executor.execute();
        a2executor.execute();
        
        TemporalGroupingQueryExecutor b1executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("YTTP2022")
            .groupByPeriodOverlaps("tpep", 
                Timestamp.valueOf("2022-01-02 00:00:00"), 
                Timestamp.valueOf("2022-01-02 16:45:36"))
            .withSelectClause(
                new DtlExpression("#total_amount",
                    new int[]{16}, 
                    new String[]{"total_amount"}), 
                new Text("sum(total_amount)"), 
                new Text("double"), 
                new Text("sum")
            )
            .build();
        b1executor.execute();
        b1executor.execute();
        b1executor.execute();
        
        TemporalGroupingQueryExecutor b2executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("YTTP2022")
            .groupByPeriodOverlaps("tpep", 
                Timestamp.valueOf("2022-12-25 00:00:00"), 
                Timestamp.valueOf("2022-12-25 16:45:36"))
            .withSelectClause(
                new DtlExpression("#total_amount",
                    new int[]{16}, 
                    new String[]{"total_amount"}), 
                new Text("sum(total_amount)"), 
                new Text("double"), 
                new Text("sum")
            )
            .build();
        b2executor.execute();
        b2executor.execute();
        b2executor.execute();
        
        TemporalGroupingQueryExecutor b3executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("YTTP2022")
            .groupByPeriodOverlaps("tpep", 
                Timestamp.valueOf("2022-01-02 00:00:00"), 
                Timestamp.valueOf("2022-01-05 23:36:00"))
            .withSelectClause(
                new DtlExpression("#total_amount",
                    new int[]{16}, 
                    new String[]{"total_amount"}), 
                new Text("sum(total_amount)"), 
                new Text("double"), 
                new Text("sum")
            )
            .build();
        b3executor.execute();
        b3executor.execute();
        b3executor.execute();
        
        TemporalGroupingQueryExecutor b4executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("YTTP2022")
            .groupByPeriodOverlaps("tpep", 
                Timestamp.valueOf("2022-12-24 00:00:00"), 
                Timestamp.valueOf("2022-12-27 23:36:00"))
            .withSelectClause(
                new DtlExpression("#total_amount",
                    new int[]{16}, 
                    new String[]{"total_amount"}), 
                new Text("sum(total_amount)"), 
                new Text("double"), 
                new Text("sum")
            )
            .build();
        b4executor.execute();
        b4executor.execute();
        b4executor.execute();
        
        TemporalGroupingQueryExecutor b5executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("YTTP2022")
            .groupByPeriodOverlaps("tpep", 
                Timestamp.valueOf("2022-01-01 00:00:00"), 
                Timestamp.valueOf("2023-01-01 00:00:00"))
            .withSelectClause(
                new DtlExpression("#total_amount",
                    new int[]{16}, 
                    new String[]{"total_amount"}), 
                new Text("sum(total_amount)"), 
                new Text("double"), 
                new Text("sum")
            )
            .build();
        b5executor.execute();
        b5executor.execute();
        b5executor.execute();
        
        TemporalGroupingQueryExecutor c1executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("YTTP2022")
            .groupByPeriodOverlaps("tpep", 
                Timestamp.valueOf("2022-06-24 00:00:00"), 
                Timestamp.valueOf("2022-06-24 16:45:36"))
            .withSelectClause(
                new DtlExpression("#tpep_pickup_datetime",
                    new int[]{1}, 
                    new String[]{"tpep_pickup_datetime"}), 
                new Text("max(tpep_pickup_datetime)"), 
                new Text("timestamp"), 
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#tpep_dropoff_datetime",
                    new int[]{2}, 
                    new String[]{"tpep_dropoff_datetime"}), 
                new Text("min(tpep_dropoff_datetime)"), 
                new Text("timestamp"), 
                new Text("min")
            )
            .withSelectClause(
                new DtlExpression("#passenger_count",
                    new int[]{3}, 
                    new String[]{"passenger_count"}), 
                new Text("max(passenger_count)"), 
                new Text("double"), 
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#trip_distance",
                    new int[]{4}, 
                    new String[]{"trip_distance"}), 
                new Text("min(trip_distance)"), 
                new Text("double"), 
                new Text("min")
            )
            .withSelectClause(
                new DtlExpression("#fare_amount",
                    new int[]{10}, 
                    new String[]{"fare_amount"}), 
                new Text("max(fare_amount)"), 
                new Text("double"), 
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#extra",
                    new int[]{11}, 
                    new String[]{"extra"}), 
                new Text("min(extra)"), 
                new Text("double"), 
                new Text("min")
            )
            .withSelectClause(
                new DtlExpression("#mta_tax",
                    new int[]{12}, 
                    new String[]{"mta_tax"}), 
                new Text("max(mta_tax)"), 
                new Text("double"), 
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#tip_amount",
                    new int[]{13}, 
                    new String[]{"tip_amount"}), 
                new Text("max(tip_amount)"), 
                new Text("double"), 
                new Text("max")
            )
            .build();
        c1executor.execute();
        c1executor.execute();
        c1executor.execute();
    }
    
}
