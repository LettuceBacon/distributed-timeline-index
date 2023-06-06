package xyz.mfj.query;

import java.io.IOException;
import java.sql.Timestamp;
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
import xyz.mfj.dataManipulation.TemporalOrcOutputReducer;
import xyz.mfj.dataQuery.DtlExpression;
import xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecuter;
import xyz.mfj.dataQuery.timeTravelQuey.TimeTravelQueryExecutorBuilder;
import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;

public class TimeTravelOnYTTRDebug {
    // 不同的运行模式，local是运行在mapreduce的local框架上，yarn是yarn框架上，mapreduce.framework.name
    // private static final String RUNNING_MODEL = "local";
    private static final String RUNNING_MODEL = "yarn";
    
    @BeforeEach
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
            inputPathStr = "/home/mfj/YTTR/yellow_tripdata_2022/yellow_tripdata_2022-01.csv";
            // inputPathStr = "/home/mfj/YTTR/yellow_tripdata_2022";
        }
        if (RUNNING_MODEL.equals("yarn")) {
            // inputPathStr = "/user/mfj/input/yellow_tripdata_2022/yellow_tripdata_2022-01.csv";
            inputPathStr = "/user/mfj/input/yellow_tripdata_2022";
        }
        
        LoadDataExecutor executor = new LoadDataExecutorBuilder()
            .intoTable("YTTP2022")
            .fromPath(inputPathStr, "csv")
            .withParallelNum(2)
            .build();
            
        executor.execute();
    }
    
    @Test
    public void timeTravelQueryDebug() throws ClassNotFoundException, IOException, InterruptedException {
        TimeTravelQueryExecutorBuilder builder = new TimeTravelQueryExecutorBuilder()
            .fromTable("YTTP2022")
            .periodContainsTime("tpep", Timestamp.valueOf("2022-01-03 00:00:00"))
            .withWhereClause(
                new DtlExpression("#VendorID == 2", 
                    new int[]{0}, 
                    new String[]{"VendorID"}), 
                new Consumer<SearchArgument.Builder>() {

                    @Override
                    public void accept(Builder arg0) {
                        arg0.equals("VendorID", PredicateLeaf.Type.LONG, Long.valueOf(2L));
                    }
                    
                })
            .withSelectClause(
                new DtlExpression("#VendorID", 
                    new int[]{0}, 
                    new String[]{"VendorID"}),
                new Text("VendorID"),
                new Text("bigint"),
                null
            )
            .withSelectClause(
                new DtlExpression("#tpep_pickup_datetime", 
                    new int[]{1}, 
                    new String[]{"tpep_pickup_datetime"}),
                new Text("tpep_pickup_datetime"),
                new Text("timestamp"),
                null
            )
            .withSelectClause(
                new DtlExpression("#tpep_dropoff_datetime", 
                    new int[]{2}, 
                    new String[]{"tpep_dropoff_datetime"}),
                new Text("tpep_dropoff_datetime"),
                new Text("timestamp"),
                null
            )
            .withSelectClause(
                new DtlExpression("#passenger_count", 
                    new int[]{3}, 
                    new String[]{"passenger_count"}),
                new Text("passenger_count"),
                new Text("double"),
                null
            )
            .withSelectClause(
                new DtlExpression("#trip_distance", 
                    new int[]{4}, 
                    new String[]{"trip_distance"}),
                new Text("trip_distance"),
                new Text("double"),
                null
            )
            // .withProperty("mapreduce.input.fileinputformat.split.minsize", "67108864")
            ;
        
        TimeTravelQueryExecuter executer = builder.build();
        executer.execute();
        
    }
    
    @Test
    public void TimeTravelQueryWithSumMaxAggDebug() 
        throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException 
    {
        TimeTravelQueryExecuter executor = new TimeTravelQueryExecutorBuilder()
            .fromTable("YTTP2022")
            .periodContainsTime("tpep", Timestamp.valueOf("2022-01-03 00:00:00"))
            .withWhereClause(
                new DtlExpression("#trip_distance > 3", 
                    new int[]{4}, 
                    new String[]{"trip_distance"}),
                new Consumer<SearchArgument.Builder>() {

                    @Override
                    public void accept(Builder builder) {
                        builder.startNot()
                            .lessThanEquals("trip_distance", PredicateLeaf.Type.FLOAT, Double.valueOf(3))
                            .end(); // end not
                    }
                    
                }
            )
            .withSelectClause(
                new DtlExpression("#total_amount", 
                    new int[]{16}, 
                    new String[]{"total_amount"}),
                new Text("max(total_amount)"),
                new Text("double"),
                new Text("max")
            )
            .withSelectClause(
                new DtlExpression("#fare_amount", 
                    new int[]{10}, 
                    new String[]{"fare_amount"}),
                new Text("sum(fare_amount)"),
                new Text("double"),
                new Text("sum")
            )
            .build();

        executor.execute();
    }
}
