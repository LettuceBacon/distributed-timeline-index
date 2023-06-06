package xyz.mfj.dataQuery.temporalGroupingQuery;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.DtlConf;
import xyz.mfj.dataDefiniation.ApplicationPeriod;
import xyz.mfj.dataDefiniation.OrcTable;
import xyz.mfj.dataQuery.DtlExpression;
import xyz.mfj.dataQuery.TemporalOrcInputFormat;
import xyz.mfj.enhanced.TypeDescriptionEnhance;
import xyz.mfj.utils.SerDeUtil;
import xyz.mfj.utils.TypeUtil;

public class TemporalGroupingQueryExecutor {
    private static Logger LOG = LoggerFactory.getLogger(TemporalGroupingQueryExecutor.class);
    
    private OrcTable table; // 内部包含Timeline index
    private Object startTime;
    private Object endTime;
    private ApplicationPeriod appPrd;
    private DtlExpression whereExpr;
    private Consumer<SearchArgument.Builder> whereSargAppender;
    private DtlExpression[] selectExprs;
    private Text[] selectNames;
    private Text[] selectTypeDescs;
    private Text[] aggregatorNames;
    private Integer numReduce;
    
    public TemporalGroupingQueryExecutor(OrcTable table,
        ApplicationPeriod appPrd,
        DtlExpression whereExpr,
        Consumer<SearchArgument.Builder> whereSargAppender,
        DtlExpression[] selectExprs,
        Text[] selectNames,
        Text[] selectTypeDescs,
        Text[] aggregatorNames,
        Object startTime,
        Object endTime,
        Integer numReduce
    ) {
        this.table = table;
        this.appPrd = appPrd;
        this.whereExpr = whereExpr;
        this.whereSargAppender = whereSargAppender;
        this.selectExprs = selectExprs;
        this.selectNames = selectNames;
        this.selectTypeDescs = selectTypeDescs;
        this.aggregatorNames = aggregatorNames;
        this.startTime = startTime;
        this.endTime = endTime;
        this.numReduce = numReduce;
    }
    
    // Distributed Timeline
    public void execute() throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        long sTime = System.currentTimeMillis();
        
        Configuration tblConf = table.getConf();
        
        Configuration jobConf = new Configuration(tblConf);
        
        TypeDescription innerSchema = table.getInnerSchema();
        innerSchema.getId();
        OrcConf.MAPRED_INPUT_SCHEMA.setString(jobConf, innerSchema.toString());
        
        boolean[] includedCols = buildIncludedCols(innerSchema.getMaximumId() + 1);
        OrcConf.INCLUDE_COLUMNS.setString(jobConf, SerDeUtil.serialize(includedCols));
        
        SearchArgument stripeFilter = buildStripeFilter(innerSchema);
        OrcConf.KRYO_SARG.setString(jobConf, SerDeUtil.serialize(stripeFilter));
        
        ArrayList<ApplicationPeriod> appPrds = new ArrayList<>();
        appPrds.add(appPrd);
        jobConf.set(
            DtlConf.APPLICATION_PERIOD, SerDeUtil.serialize(appPrds)
        );
        
        if (startTime == null) {
            startTime = TypeUtil.minOf(
                TypeDescriptionEnhance.getNameAndType(
                    innerSchema, appPrd.getAppPrdSId() + 1
                ).getRight()
            );
        }
        if (endTime == null) {
            endTime = TypeUtil.maxOf(
                TypeDescriptionEnhance.getNameAndType(
                    innerSchema, appPrd.getAppPrdEId() + 1
                ).getRight()
            );
        }
        String shuffleKeySchema = null;
        long periodRange; // 用于计算时间窗口长度
        if (startTime.getClass().equals(Timestamp.class)) {
            Timestamp sTimestamp = (Timestamp)startTime;
            Timestamp eTimestamp = (Timestamp)endTime;
            jobConf.set(DtlConf.OVERLAPS_START_TIME, sTimestamp.toString());
            jobConf.set(DtlConf.OVERLAPS_END_TIME, eTimestamp.toString());
            shuffleKeySchema = "timestamp";
            periodRange = eTimestamp.getTime() - sTimestamp.getTime();
        }
        else {
            LocalDate sDate = (LocalDate)startTime;
            LocalDate eDate = (LocalDate)endTime;
            jobConf.set(DtlConf.OVERLAPS_START_TIME, sDate.toString());
            jobConf.set(DtlConf.OVERLAPS_END_TIME, eDate.toString());
            shuffleKeySchema = "date";
            periodRange = eDate.toEpochDay() - sDate.toEpochDay();
        }
        jobConf.set(DtlConf.PERIOD_ENDPOINT_TYPE, shuffleKeySchema);
        OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.setString(jobConf, shuffleKeySchema);
        // 设定一个时间窗口长度，在部分聚合时，首先读取到startTime，在每个时间点上聚合，设置key为时间点除以时间窗口的值，当key与前一个key不同时，根据情况复制切点的数据，
        // 如果key是date，windowlen是epochday，只需要将key转换一下变成子类就可以计算了
        
        if (whereExpr != null) {
            DefaultStringifier.store(jobConf, whereExpr, DtlConf.WHERE_EXPR);
        }
        DefaultStringifier.storeArray(jobConf, selectExprs, DtlConf.SELECT_EXPR);
        
        TypeDescription partRsSchm = TypeDescription.createStruct();
        for (int i = 0; i < selectNames.length; ++i) {
            TypeDescriptionEnhance.structAddField(
                partRsSchm, 
                selectNames[i].toString(), 
                TypeDescription.fromString(selectTypeDescs[i].toString())
            );
        }
        // 输出结果的模式
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(jobConf, partRsSchm.toString());
        // "1_0" "1_1" "2_0" "1_0" "mapperId_stripeId"
        TypeDescriptionEnhance.structAddField(
            partRsSchm, 
            "mapperIdStripeId",
            TypeDescription.createString()
        );
        OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.setString(jobConf, partRsSchm.toString());
        
        DefaultStringifier.storeArray(jobConf, aggregatorNames, DtlConf.SELECT_TEMPORAL_AGGS);
        
        String maxTaskMem = jobConf.get(DtlConf.MAX_TASK_MEM);
        if (maxTaskMem != null) {
            jobConf.set("mapreduce.map.java.opts", "-Xmx" + maxTaskMem);
        }
        
        Job job = Job.getInstance(jobConf, "temporal-grouping");
        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TemporalOrcInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(PartialComputeMapper.class);
        job.setReducerClass(MergeReducer.class);
        job.setMapOutputKeyClass(OrcKey.class);
        job.setMapOutputValueClass(OrcValue.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        if (numReduce != null) {
            job.setPartitionerClass(WindowPartitioner.class);
            job.setNumReduceTasks(numReduce);
            // 窗口长度是（endTime-startTime）/numreduce + 1
            job.getConfiguration().setLong(DtlConf.WINDOW_LENGTH, periodRange/ numReduce + 1);
        }
        
        List<Path> paths = table.getStoragePath();
        for (Path path : paths) {
            FileInputFormat.addInputPath(job, path);
        }
        Path fooPath = new Path("foo");
        FileOutputFormat.setOutputPath(job, fooPath);
        job.waitForCompletion(true);
        FileSystem fs = FileSystem.get(jobConf);
        if (fs.exists(fooPath)) {
            fs.delete(fooPath, true);
        }
        
        long eTime = System.currentTimeMillis();
        LOG.info("Time taken {} ms", eTime - sTime);
    
        CounterGroup userCounters = job.getCounters().getGroup("user");
        for (Counter counter : userCounters) {
            LOG.info("{} = {} ", counter.getName(), counter.getValue());
        }
    }
    
    private boolean[] buildIncludedCols(int n) {
        boolean[] includedCols = new boolean[n];
        includedCols[appPrd.getAppPrdSId() + 1] = true;
        includedCols[appPrd.getAppPrdEId() + 1] = true;
        includedCols[appPrd.getRf0Id() + 1] = true;
        includedCols[appPrd.getRf1Id() + 1] = true;
        int[] ids = null;
        if (whereExpr != null) {
            ids = whereExpr.getIncludedColIds();
            for (int id : ids) {
                includedCols[id + 1] = true;
            }
        }
        for (DtlExpression selectExpr : selectExprs) {
            ids = selectExpr.getIncludedColIds();
            for (int id : ids) {
                includedCols[id + 1] = true;
            }
        }
        includedCols[0] = true;
        return includedCols;
    }
    
    private SearchArgument buildStripeFilter(TypeDescription innerSchema) {
        Pair<String, TypeDescription> prdSNameType = 
            TypeDescriptionEnhance.getNameAndType(innerSchema, appPrd.getAppPrdSId() + 1);
        Pair<String, TypeDescription> prdENameType = 
            TypeDescriptionEnhance.getNameAndType(innerSchema, appPrd.getAppPrdEId() + 1);
        // 将`OVERLAPS`和`WHERE`用`AND`连接作为条带的筛选条件
        SearchArgument.Builder filterBuilder = SearchArgumentFactory.newBuilder().startAnd();
        if (startTime == null && endTime == null) { // 查询整个时间线
            this.startTime = TypeUtil.minOf(prdSNameType.getRight());
            this.endTime = TypeUtil.maxOf(prdENameType.getRight());
        }
        else { // startTime != null && endTime != null 查询部分时间线
            this.startTime = startTime;
            this.endTime = endTime;
        }
        
        // periodStart < endTime AND periodEnd > startTime
        Object sTime = startTime.getClass() == Timestamp.class ? startTime
            : Date.valueOf((LocalDate)startTime);
        Object eTime = endTime.getClass() == Timestamp.class ? endTime
            : Date.valueOf((LocalDate)endTime);
        filterBuilder.startAnd()
            .lessThan(prdSNameType.getLeft(), 
                TypeUtil.typeDesc2PredLeafType(prdSNameType.getRight()), 
                eTime) // periodStart < endTime
            .startNot()
            .lessThanEquals(prdENameType.getLeft(), 
                TypeUtil.typeDesc2PredLeafType(prdENameType.getRight()),
                sTime) // periodEnd > startTime
            .end() // end not
            .end(); // end and
        if (whereSargAppender != null) {
            whereSargAppender.accept(filterBuilder);
        }
        
        return filterBuilder.end().build();
    }
    
    
    // // ParTime
    // // 对于cumulativeAggregation，遍历整个stripe，生成每个startTime和endTime与聚合表达式们的键值对，被where过滤掉的不会对最终结果产生影响不需要产生键值对，然后在merge时将这个键值对按键的顺序归并，同时累积
    // // 对于selectiveAggregation，遍历整个stripe，生成每个startTime和endTime与变得有效和变得无效的值，被where过滤的不需要，然后在merge时建立一个优先队列，按照键顺序放入变得有效的，并去掉变得无效的，并在每个时间点从优先队列获得最值
    
    // // mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue>，outputKey是时间，outputValue是一个struct，根据agg的不同由不同的元素构成，如果是累积型，则是一个primitive类型，如果是选择型，两个OrcList组成的OrcStruct
    
    // public void execute() 
    //     throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException 
    // {
    //     long sTime = System.currentTimeMillis();
        
    //     Configuration tblConf = table.getConf();
        
    //     Configuration jobConf = new Configuration(tblConf);
        
    //     TypeDescription innerSchema = table.getInnerSchema();
    //     innerSchema.getId();
    //     OrcConf.MAPRED_INPUT_SCHEMA.setString(jobConf, innerSchema.toString());
        
    //     boolean[] includedCols = buildIncludedCols(innerSchema.getMaximumId() + 1);
    //     OrcConf.INCLUDE_COLUMNS.setString(jobConf, SerDeUtil.serialize(includedCols));
        
    //     SearchArgument stripeFilter = buildStripeFilter(innerSchema);
    //     OrcConf.KRYO_SARG.setString(jobConf, SerDeUtil.serialize(stripeFilter));
        
    //     ArrayList<ApplicationPeriod> appPrds = new ArrayList<>();
    //     appPrds.add(appPrd);
    //     jobConf.set(
    //         DtlConf.APPLICATION_PERIOD, SerDeUtil.serialize(appPrds)
    //     );
        
    //     if (startTime == null) {
    //         startTime = TypeUtil.minOf(
    //             TypeDescriptionEnhance.getNameAndType(
    //                 innerSchema, appPrd.getAppPrdSId() + 1
    //             ).getRight()
    //         );
    //     }
    //     if (endTime == null) {
    //         endTime = TypeUtil.maxOf(
    //             TypeDescriptionEnhance.getNameAndType(
    //                 innerSchema, appPrd.getAppPrdEId() + 1
    //             ).getRight()
    //         );
    //     }
    //     String shuffleKeySchema = null;
    //     if (startTime.getClass().equals(Timestamp.class)) {
    //         Timestamp sTimestamp = (Timestamp)startTime;
    //         Timestamp eTimestamp = (Timestamp)endTime;
    //         jobConf.set(DtlConf.OVERLAPS_START_TIME, sTimestamp.toString());
    //         jobConf.set(DtlConf.OVERLAPS_END_TIME, eTimestamp.toString());
    //         shuffleKeySchema = "timestamp";
    //     }
    //     else {
    //         LocalDate sDate = (LocalDate)startTime;
    //         LocalDate eDate = (LocalDate)endTime;
    //         jobConf.set(DtlConf.OVERLAPS_START_TIME, sDate.toString());
    //         jobConf.set(DtlConf.OVERLAPS_END_TIME, eDate.toString());
    //         shuffleKeySchema = "date";
    //     }
    //     jobConf.set(DtlConf.PERIOD_ENDPOINT_TYPE, shuffleKeySchema);
    //     OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.setString(jobConf, shuffleKeySchema);
        
    //     if (whereExpr != null) {
    //         DefaultStringifier.store(jobConf, whereExpr, DtlConf.WHERE_EXPR);
    //     }
    //     DefaultStringifier.storeArray(jobConf, selectExprs, DtlConf.SELECT_EXPR);
        
    //     TypeDescription partRsSchm = TypeDescription.createStruct();
    //     for (int i = 0; i < selectNames.length; ++i) {
    //         TypeDescriptionEnhance.structAddField(
    //             partRsSchm, 
    //             selectNames[i].toString(), 
    //             TypeDescription.fromString(selectTypeDescs[i].toString())
    //         );
    //     }
    //     // 输出结果的模式
    //     OrcConf.MAPRED_OUTPUT_SCHEMA.setString(jobConf, partRsSchm.toString());
        
    //     // 根据agg的不同生成混洗过程的数据的模式，即deltaValues的模式
    //     TypeDescription shuffleValueSchema = TypeDescription.createStruct();
    //     for (int i = 0; i < aggregatorNames.length; ++i) {
    //         switch (aggregatorNames[i].toString().toLowerCase()) {
    //             case "sum":
    //                 TypeDescriptionEnhance.structAddField(
    //                     shuffleValueSchema, 
    //                     selectNames[i].toString(), 
    //                     TypeDescription.fromString(selectTypeDescs[i].toString())
    //                 );
    //                 break;
                    
    //             case "max":
    //             case "min":
    //                 TypeDescriptionEnhance.structAddField(
    //                     shuffleValueSchema, 
    //                     selectNames[i].toString(), 
    //                     TypeDescription.createStruct()
    //                         .addField(
    //                             "becomeValidSet", 
    //                             TypeDescription.createList(
    //                                 TypeDescription.fromString(selectTypeDescs[i].toString())
    //                             )
    //                         )
    //                         .addField(
    //                             "becomeInvalidSet", 
    //                             TypeDescription.createList(
    //                                 TypeDescription.fromString(selectTypeDescs[i].toString())
    //                             )
    //                         )
    //                 );
    //                 break;
            
    //             default:
    //                 throw new UnsupportedOperationException(
    //                     String.format("Unimplemented aggregator '%s'", aggregatorNames[i].toString())
    //                 );
    //         }
    //     }
    //     OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.setString(jobConf, shuffleValueSchema.toString());
        
    //     DefaultStringifier.storeArray(jobConf, aggregatorNames, DtlConf.SELECT_TEMPORAL_AGGS);
        
    //     String maxTaskMem = jobConf.get(DtlConf.MAX_TASK_MEM);
    //     if (maxTaskMem != null) {
    //         jobConf.set("mapreduce.map.java.opts", "-Xmx" + maxTaskMem);
    //     }
        
    //     Job job = Job.getInstance(jobConf, "temporal-grouping");
    //     job.setJarByClass(this.getClass());
    //     job.setInputFormatClass(TemporalOrcInputFormat.class);
    //     job.setOutputFormatClass(TextOutputFormat.class);
    //     job.setMapperClass(PartialComputeMapper.class);
    //     job.setReducerClass(MergeReducer.class);
    //     job.setMapOutputKeyClass(OrcKey.class);
    //     job.setMapOutputValueClass(OrcValue.class);
    //     job.setOutputKeyClass(NullWritable.class);
    //     job.setOutputValueClass(NullWritable.class);
        
    //     List<Path> paths = table.getStoragePath();
    //     for (Path path : paths) {
    //         FileInputFormat.addInputPath(job, path);
    //     }
    //     Path fooPath = new Path("foo");
    //     FileOutputFormat.setOutputPath(job, fooPath);
    //     job.waitForCompletion(true);
    //     FileSystem fs = FileSystem.get(jobConf);
    //     if (fs.exists(fooPath)) {
    //         fs.delete(fooPath, true);
    //     }
        
    //     long eTime = System.currentTimeMillis();
    //     LOG.info("Time taken {} ms", eTime - sTime);
        
    //     CounterGroup userCounters = job.getCounters().getGroup("user");
    //     for (Counter counter : userCounters) {
    //         LOG.info("{} = {} ", counter.getName(), counter.getValue());
    //     }
    // }
}
