package xyz.mfj.dataQuery.timeTravelQuey;

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
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcTimestamp;
import org.apache.orc.mapred.OrcValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.DtlConf;
// import net.sourceforge.sizeof.SizeOf;
import xyz.mfj.dataDefiniation.ApplicationPeriod;
import xyz.mfj.dataDefiniation.OrcTable;
import xyz.mfj.dataQuery.DtlExpression;
import xyz.mfj.dataQuery.TemporalOrcInputFormat;
import xyz.mfj.enhanced.TypeDescriptionEnhance;
import xyz.mfj.utils.SerDeUtil;
import xyz.mfj.utils.TypeUtil;

// 这是使用timeline实现时间旅行的类
// 使用orcColumnStatisitc可以用Hive模拟
public class TimeTravelQueryExecuter {
    private static final Logger LOG = LoggerFactory.getLogger(TimeTravelQueryExecuter.class);
    
    private OrcTable table; // 内部包含Timeline index
    private Object containsTime;
    // private WritableComparable containsTime;
    private ApplicationPeriod appPrd;
    private DtlExpression whereExpr;
    private Consumer<SearchArgument.Builder> whereSargAppender;
    private DtlExpression[] selectExprs;
    private Text[] selectNames;
    private Text[] selectTypeDescs;
    private Text[] aggregatorNames;
    private List<Pair<String,String>> properties;
    
    public TimeTravelQueryExecuter(OrcTable table,
        ApplicationPeriod appPrd,
        DtlExpression whereExpr,
        Consumer<SearchArgument.Builder> whereSargAppender,
        DtlExpression[] selectExprs,
        Text[] selectNames,
        Text[] selectTypeDescs,
        Text[] aggregatorNames,
        Object containsTime,
        List<Pair<String,String>> properties
    ) {
        this.table = table;
        this.appPrd = appPrd;
        this.whereExpr = whereExpr;
        this.whereSargAppender = whereSargAppender;
        this.selectExprs = selectExprs;
        this.selectNames = selectNames;
        this.selectTypeDescs = selectTypeDescs;
        this.aggregatorNames = aggregatorNames;
        this.containsTime = containsTime;
        this.properties = properties;
    }
    
    public void execute() throws IOException, ClassNotFoundException, InterruptedException {
        long sTime = System.currentTimeMillis();
        
        Configuration tblConf = table.getConf();
        
        Configuration jobConf = new Configuration(tblConf);
        
        if (properties != null) {
            for (Pair<String, String> property : properties) {
                jobConf.set(property.getLeft(), property.getRight());
            }    
        }
        
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
        
        String shuffleKeySchemaStr = null;
        if (containsTime.getClass().equals(Timestamp.class)) {
            jobConf.set(DtlConf.CONTAINS_TIME, ((Timestamp)containsTime).toString());
            shuffleKeySchemaStr = "timestamp";
        }
        else {
            jobConf.set(DtlConf.CONTAINS_TIME, ((LocalDate)containsTime).toString());
            shuffleKeySchemaStr = "date";
        }
        jobConf.set(DtlConf.PERIOD_ENDPOINT_TYPE, shuffleKeySchemaStr);
        OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.setString(jobConf, shuffleKeySchemaStr);
        
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
        OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.setString(jobConf, partRsSchm.toString());
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(jobConf, partRsSchm.toString());
        
        if (aggregatorNames != null) {
            DefaultStringifier.storeArray(jobConf, aggregatorNames, DtlConf.SELECT_TEMPORAL_AGGS);
        }
        
        String maxTaskMem = jobConf.get(DtlConf.MAX_TASK_MEM);
        if (maxTaskMem != null) {
            jobConf.set("mapreduce.map.java.opts", "-Xmx" + maxTaskMem);
        }
        

        Job job = Job.getInstance(jobConf, "time-travel");
        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TemporalOrcInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(PartialComputeMapper.class);
        job.setReducerClass(MergeReducer.class);
        job.setMapOutputKeyClass(OrcKey.class);
        job.setMapOutputValueClass(OrcValue.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
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
        Pair<String, TypeDescription> prdStartNameType = 
            TypeDescriptionEnhance.getNameAndType(innerSchema, appPrd.getAppPrdSId() + 1);
        Pair<String, TypeDescription> prdEndNameType = 
            TypeDescriptionEnhance.getNameAndType(innerSchema, appPrd.getAppPrdEId() + 1);
        // 将`PERIOD CONTAINS`和`WHERE`用`AND`连接作为条带的筛选条件
        SearchArgument.Builder filterBuilder = SearchArgumentFactory.newBuilder().startAnd();
        Object cTime = containsTime.getClass() == Timestamp.class ? containsTime
            : Date.valueOf((LocalDate)containsTime);
        // startTime <= containsTime AND endTime > containsTime
        filterBuilder.startAnd()
            .lessThanEquals(prdStartNameType.getLeft(), 
                TypeUtil.typeDesc2PredLeafType(prdStartNameType.getRight()), 
                cTime) // startTime <= containsTime
            .startNot()
            .lessThanEquals(prdEndNameType.getLeft(), 
                TypeUtil.typeDesc2PredLeafType(prdEndNameType.getRight()),
                cTime) // endTime > containsTime
            .end() // end not
            .end(); // end and
        if (whereSargAppender != null) {
            whereSargAppender.accept(filterBuilder);
        }
        
        return filterBuilder.end().build();
    }
    
}
