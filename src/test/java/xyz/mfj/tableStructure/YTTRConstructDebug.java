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
import xyz.mfj.dataManipulation.LoadDataExecutor;
import xyz.mfj.dataManipulation.LoadDataExecutorBuilder;
import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;

public class YTTRConstructDebug {
    // 不同的运行模式，local是运行在mapreduce的local框架上，yarn是yarn框架上，mapreduce.framework.name
    private static final String RUNNING_MODEL = "local";
    // private static final String RUNNING_MODEL = "yarn";
    
    @Test
    public void constructYTTRDebug() 
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
            
        String inputPathStr = null;
        if (RUNNING_MODEL.equals("local")) {
            // inputPathStr = "/home/mfj/YTTR/yellow_tripdata_2022/yellow_tripdata_2022-01.csv";
            inputPathStr = "/home/mfj/YTTR/yellow_tripdata_2022";
        }
        if (RUNNING_MODEL.equals("yarn")) {
            inputPathStr = "/user/mfj/input/yellow_tripdata_2022";
        }
        
        LoadDataExecutor executor = new LoadDataExecutorBuilder()
            .intoTable("YTTP2022")
            .fromPath(inputPathStr, "csv")
            // .withProperties(TemporalOrcOutputReducer.NULLSTRING_CONF_NAME, "")
            // .withParallelNum(2)
            .build();
            
        executor.execute();
    }
}
