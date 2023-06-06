package xyz.mfj;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
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
import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;

public class LoadData {
    private static final Logger LOG = LoggerFactory.getLogger(LoadData.class);
    
    // 实验不同排序算法的执行时间和内存占用
    // local模式执行
    // 测试内存需添加SizeOf依赖，在settings.json中添加--add-opens
    @Test
    public void sortAlgorithmPerformance() 
        throws ClassNotFoundException, IOException, InterruptedException 
    {
        Library lib = Library.getInstance();
        Configuration libConf = lib.getConf();

        // // tpc-bih sf1 lineitem
        // // 表配置
        // Configuration tblConf = new Configuration(libConf);
        // OrcConf.COMPRESS.setString(tblConf, "NONE");
        // // 64MB
        // tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        // OrcConf.ROW_BATCH_SIZE.setInt(tblConf, EnhancedVectorizedRowBatch.INCREMENT_SIZE);
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1070000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1070000);

        // // 建表
        // OrcTable lineitembih = new OrcTableBuilder()
        //     .createTable("lineitembih")
        //     .addColumn("orderKey", TypeDescription.createLong())
        //     .addColumn("partKey", TypeDescription.createLong())
        //     .addColumn("supplierKey", TypeDescription.createLong())
        //     .addColumn("lineNumber", TypeDescription.createInt())
        //     .addColumn("quantity", TypeDescription.createDecimal())
        //     .addColumn("extendedPrice", TypeDescription.createDecimal().withPrecision(10).withScale(2))
        //     .addColumn("discount", TypeDescription.createDecimal().withPrecision(10).withScale(2))
        //     .addColumn("tax", TypeDescription.createDecimal().withPrecision(10).withScale(2))
        //     .addColumn("returnFlag", TypeDescription.createChar().withMaxLength(3))
        //     .addColumn("status", TypeDescription.createChar().withMaxLength(3))
        //     .addColumn("shipDate", TypeDescription.createDate())
        //     .addColumn("commitDate", TypeDescription.createDate())
        //     .addColumn("receiptDate", TypeDescription.createDate())
        //     .addColumn("shipInstructions", TypeDescription.createChar().withMaxLength(25))
        //     .addColumn("shipMode", TypeDescription.createChar().withMaxLength(10))
        //     .addColumn("comment", TypeDescription.createVarchar().withMaxLength(44))
        //     .addColumn("activeTimeBegin", TypeDescription.createDate())
        //     .addColumn("activeTimeEnd", TypeDescription.createDate())
        //     .periodFor("activeTime", "activeTimeBegin", "activeTimeEnd")
        //     .withConf(tblConf)
        //     .build();
        // lib.cacheTable(lineitembih);
            
        // String inputPathStr = "/home/mfj/lineitembih/lineitembih-part-1-sf-1.dat";
        
        // LoadDataExecutor executor = new LoadDataExecutorBuilder()
        //     .intoTable("lineitembih")
        //     .fromPath(inputPathStr, "csv")
        //     .withProperties(CsvInputMapper.SEPARATORCHAR_CONF_NAME, "|")
        //     // .withParallelNum(2)
        //     .build();
            
        // executor.execute();
        // executor.execute();
        // executor.execute();
        

        // YTTP2022
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
            
        String inputPathStr = "/home/mfj/YTTR/yellow_tripdata_2022";
        
        LoadDataExecutor executor = new LoadDataExecutorBuilder()
            .intoTable("YTTP2022")
            .fromPath(inputPathStr, "csv")
            .build();
            
        executor.execute();
        executor.execute();
        executor.execute();
    }
        
    @Test
    public void loadDataDebug() 
        throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException 
    {
        Library lib = Library.getInstance();
        Configuration libConf = lib.getConf();
        
        libConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", Boolean.FALSE.toString());
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/core-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/hdfs-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/yarn-site.xml"));
        libConf.addResource(new Path("/home/mfj/hadoop/hadoop-3.3.4/etc/hadoop/mapred-site.xml"));
        
        // // tpc-bih sf1 lineitem
        // // 表配置
        // Configuration tblConf = new Configuration(libConf);
        // OrcConf.COMPRESS.setString(tblConf, "NONE");
        // // 64MB
        // tblConf.set(DtlConf.MAX_TASK_MEM, "2048M");
        // OrcConf.ROW_BATCH_SIZE.setInt(tblConf, EnhancedVectorizedRowBatch.INCREMENT_SIZE);
        // OrcConf.STRIPE_ROW_COUNT.setLong(tblConf, 1070000);
        // OrcConf.ROWS_BETWEEN_CHECKS.setInt(tblConf, 1070000);

        // // 建表
        // OrcTable lineitembih = new OrcTableBuilder()
        //     .createTable("lineitembih")
        //     .addColumn("orderKey", TypeDescription.createLong())
        //     .addColumn("partKey", TypeDescription.createLong())
        //     .addColumn("supplierKey", TypeDescription.createLong())
        //     .addColumn("lineNumber", TypeDescription.createInt())
        //     .addColumn("quantity", TypeDescription.createDecimal())
        //     .addColumn("extendedPrice", TypeDescription.createDecimal().withPrecision(10).withScale(2))
        //     .addColumn("discount", TypeDescription.createDecimal().withPrecision(10).withScale(2))
        //     .addColumn("tax", TypeDescription.createDecimal().withPrecision(10).withScale(2))
        //     .addColumn("returnFlag", TypeDescription.createChar().withMaxLength(3))
        //     .addColumn("status", TypeDescription.createChar().withMaxLength(3))
        //     .addColumn("shipDate", TypeDescription.createDate())
        //     .addColumn("commitDate", TypeDescription.createDate())
        //     .addColumn("receiptDate", TypeDescription.createDate())
        //     .addColumn("shipInstructions", TypeDescription.createChar().withMaxLength(25))
        //     .addColumn("shipMode", TypeDescription.createChar().withMaxLength(10))
        //     .addColumn("comment", TypeDescription.createVarchar().withMaxLength(44))
        //     .addColumn("activeTimeBegin", TypeDescription.createDate())
        //     .addColumn("activeTimeEnd", TypeDescription.createDate())
        //     .periodFor("activeTime", "activeTimeBegin", "activeTimeEnd")
        //     .withConf(tblConf)
        //     .build();
        // lib.cacheTable(lineitembih);
            
        // String inputPathStr = "/user/mfj/input/lineitembih-part-1-sf-1.dat";
        
        // LoadDataExecutor executor = new LoadDataExecutorBuilder()
        //     .intoTable("lineitembih")
        //     .fromPath(inputPathStr, "csv")
        //     .withProperties(CsvInputMapper.SEPARATORCHAR_CONF_NAME, "|")
        //     .withParallelNum(2)
        //     .build();
            
        // executor.execute();
        // executor.execute();
        // executor.execute();
        

        // YTTP2022
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
        // executor.execute();
        // executor.execute();
    }
}
