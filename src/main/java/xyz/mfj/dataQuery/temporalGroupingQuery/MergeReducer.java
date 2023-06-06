package xyz.mfj.dataQuery.temporalGroupingQuery;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.DtlConf;
import xyz.mfj.dataDefiniation.RowVector;
import xyz.mfj.dataDefiniation.TimelineIndex;
import xyz.mfj.dataQuery.Aggregator;
import xyz.mfj.dataQuery.TemporalMaxAggregator;
import xyz.mfj.dataQuery.TemporalMinAggregator;
import xyz.mfj.dataQuery.TemporalSumAggregator;
import xyz.mfj.utils.TypeUtil;

public class MergeReducer extends Reducer<OrcKey, OrcValue, NullWritable, NullWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(PartialComputeMapper.class);
    
    // Distributed Timeline
    private TypeDescription resultSchema;
    private Aggregator[] aggregators;
    private TreeMap<String, RowVector> preRows;
    private int prtRsIdId;
    private boolean schemaPrinted;
    private OrcStruct rs;
    
    private long sTime;
    private long eTime;
    private double sumTime;
    private int reducerId;
    
    private long sNanoTime;
    private long eNanoTime;
    
    @Override
    protected void setup(Reducer<OrcKey, OrcValue, NullWritable, NullWritable>.Context context)
        throws IOException, InterruptedException 
    {
        Configuration conf = context.getConfiguration();
        resultSchema = TypeDescription.fromString(OrcConf.MAPRED_OUTPUT_SCHEMA.getString(conf));
        resultSchema.getId();
        aggregators = restoreAggregators(conf);
        preRows = new TreeMap<>();
        TypeDescription prtRsSchm = TypeDescription.fromString(
            OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.getString(conf)
        );
        prtRsSchm.getId();
        prtRsIdId = prtRsSchm.getMaximumId() - 1;
        schemaPrinted = false;
        rs = (OrcStruct)OrcStruct.createValue(resultSchema);
    
        sumTime = 0.0;
        reducerId = context.getTaskAttemptID().getTaskID().getId();
    }
    
    private Aggregator[] restoreAggregators(Configuration conf) throws IOException {
        Aggregator[] aggregators = null;
        Text[] tempAggregatorNames = null;
        List<TypeDescription> children = resultSchema.getChildren();
        tempAggregatorNames = DefaultStringifier.loadArray(
            conf, DtlConf.SELECT_TEMPORAL_AGGS, Text.class
        );
        aggregators = new Aggregator[tempAggregatorNames.length];
        for (int i = 0; i < tempAggregatorNames.length; ++i) {
            switch (tempAggregatorNames[i].toString().toLowerCase()) {
                case "sum":
                    aggregators[i] = new TemporalSumAggregator(children.get(i));
                    break;
                case "max":
                    aggregators[i] = new TemporalMaxAggregator(children.get(i));
                    break;
                case "min":
                    aggregators[i] = new TemporalMinAggregator(children.get(i));
                    break;
                    
                default:
                    LOG.error("Unsupported aggregator {}", tempAggregatorNames[i].toString());
                    System.exit(1);
                    break;
            }
        }
        return aggregators;
    }
    
    @Override
    protected void reduce(OrcKey key, Iterable<OrcValue> values,
        Reducer<OrcKey, OrcValue, NullWritable, NullWritable>.Context context)
        throws IOException, InterruptedException 
    {
        sTime = System.currentTimeMillis();
        sNanoTime = System.nanoTime();
        // 对于一个key
        // 获取每个row，查找一个row对应的mapper和stripe前一个值
        // 聚合时，collect（前一个值，invalidFlag），collect（当前值，validFlag）
        
        if (!schemaPrinted) {
            System.out.println(resultSchema);
            schemaPrinted = true;
        }
            
        for (OrcValue value : values) { 
            OrcStruct row = (OrcStruct)value.value;
            RowVector rowVec = new RowVector(aggregators.length);
            
            for (int i = 0; i < aggregators.length; ++i) {
                rowVec.set(i, TypeUtil.hadoopObj2JObj(
                    ((OrcStruct)value.value).getFieldValue(i))
                );
                aggregators[i].collect(rowVec.get(i), TimelineIndex.STARTIDX);
            }
            
            String prtRsName = ((Text)row.getFieldValue(prtRsIdId)).toString();
            RowVector preRow = preRows.get(prtRsName);
            if (preRow != null) {
                for (int i = 0; i < aggregators.length; ++i) {
                    aggregators[i].collect(preRow.get(i), TimelineIndex.ENDIDX);
                }
            }
            
            preRows.put(prtRsName, rowVec);
        }
        
        for (int i = 0; i < aggregators.length; ++i) {
            rs.setFieldValue(i, TypeUtil.jObj2hadoopObj(aggregators[i].aggregate()));
        }
        // System.out.println(rs);
        
        eTime = System.currentTimeMillis();
        eNanoTime = System.nanoTime();
        sumTime = (eTime - sTime == 0) ? 
            sumTime + (eNanoTime - sNanoTime) / 1000000.0 : sumTime + eTime - sTime;
    }
    
    @Override
    protected void cleanup(Reducer<OrcKey, OrcValue, NullWritable, NullWritable>.Context context)
        throws IOException, InterruptedException 
    {
        Counter counter = context.getCounter("user", "reducer" + reducerId);
        counter.setValue(((Double)sumTime).longValue());
        LOG.info("Merge taken {} ms", sumTime);
    }
    
    // // ParTime
    // private TypeDescription resultSchema;
    // private Aggregator[] aggregators;
    // private Text[] aggregatorNames;
    // private Comparable startTime;
    // private Comparable endTime;
    // private Comparable lastV;
    // private OrcStruct rs;
    // private boolean versionAfterEndTimeExisted;
    
    // private long sTime;
    // private long eTime;
    // private long sumTime;
    // private int reducerId;
    
    // @Override
    // protected void setup(Reducer<OrcKey, OrcValue, NullWritable, NullWritable>.Context context)
    //     throws IOException, InterruptedException 
    // {
    //     Configuration conf = context.getConfiguration();
    //     resultSchema = TypeDescription.fromString(OrcConf.MAPRED_OUTPUT_SCHEMA.getString(conf));
    //     resultSchema.getId();
    //     aggregators = restoreAggregators(conf);
    //     restoreOverlapsTime(conf);
    //     rs = (OrcStruct)OrcStruct.createValue(resultSchema);
    //     versionAfterEndTimeExisted = false;
        
    //     System.out.println(resultSchema);
    //     sumTime = 0L;
    //     reducerId = context.getTaskAttemptID().getTaskID().getId();
    // }
    
    // private Aggregator[] restoreAggregators(Configuration conf) throws IOException {
    //     Aggregator[] aggregators = null;
    //     List<TypeDescription> children = resultSchema.getChildren();
    //     aggregatorNames = DefaultStringifier.loadArray(
    //         conf, DtlConf.SELECT_TEMPORAL_AGGS, Text.class
    //     );
    //     aggregators = new Aggregator[aggregatorNames.length];
    //     for (int i = 0; i < aggregatorNames.length; ++i) {
    //         switch (aggregatorNames[i].toString().toLowerCase()) {
    //             case "sum":
    //                 aggregators[i] = new TemporalSumAggregator(children.get(i));
    //                 break;
    //             case "max":
    //                 aggregators[i] = new TemporalMaxAggregator(children.get(i));
    //                 break;
    //             case "min":
    //                 aggregators[i] = new TemporalMinAggregator(children.get(i));
    //                 break;
                    
    //             default:
    //                 throw new UnsupportedOperationException(
    //                     String.format("Unsupported aggregator %s", aggregatorNames[i].toString())
    //                 );
    //         }
    //     }
    //     return aggregators;
    // }
    
    // private void restoreOverlapsTime(Configuration conf) throws IOException {
    //     String overlapsPeriodEndpointType = conf.get(DtlConf.PERIOD_ENDPOINT_TYPE);
    //     if (overlapsPeriodEndpointType.equals("timestamp")) {
    //         startTime = Timestamp.valueOf(
    //             conf.get(DtlConf.OVERLAPS_START_TIME)
    //         );
    //         endTime = Timestamp.valueOf(
    //             conf.get(DtlConf.OVERLAPS_END_TIME)
    //         );
    //     }
    //     else {
    //         startTime = LocalDate.parse(
    //             conf.get(DtlConf.OVERLAPS_START_TIME)
    //         );
    //         endTime = LocalDate.parse(
    //             conf.get(DtlConf.OVERLAPS_END_TIME)
    //         );
    //     }
    // }
    
    // @Override
    // protected void reduce(OrcKey key, Iterable<OrcValue> values,
    //     Reducer<OrcKey, OrcValue, NullWritable, NullWritable>.Context context)
    //     throws IOException, InterruptedException 
    // {
    //     sTime = System.currentTimeMillis();
        
    //     Comparable currentV = (Comparable)TypeUtil.hadoopObj2JObj(key.key);
    //     if (lastV != null && lastV.compareTo(currentV) > 0) {
    //         LOG.error("In merge step, deltaMaps are disordered.", new RuntimeException());
    //         System.exit(1);
    //     }
        
    //     if (currentV.compareTo(startTime) <= 0) {
    //         // 只收集currentV上的值
    //         for (OrcValue value : values) {
    //             OrcStruct shuffleDeltaValues = (OrcStruct)value.value;
    //             collectDeltaValues(shuffleDeltaValues);
    //         }
    //     }
    //     else if (currentV.compareTo(startTime) > 0 && currentV.compareTo(endTime) < 0) {
    //         // 先聚合lastV上的值，再收集currentV上的值
    //         if (lastV != null) {
    //             for (int i = 0; i < aggregators.length; ++i) {
    //                 rs.setFieldValue(i, TypeUtil.jObj2hadoopObj(aggregators[i].aggregate()));
    //             }
    //             System.out.println(rs);
    //         }
            
    //         for (OrcValue value : values) {
    //             OrcStruct shuffleDeltaValues = (OrcStruct)value.value;
    //             collectDeltaValues(shuffleDeltaValues);
    //         }
    //     }
    //     else { // currentV >= endTime
    //         // 聚合lastV上的值，然后标记后面的时间什么都不做
    //         if (versionAfterEndTimeExisted == false) {
    //             if (lastV != null) {
    //                 for (int i = 0; i < aggregators.length; ++i) {
    //                     rs.setFieldValue(i, TypeUtil.jObj2hadoopObj(aggregators[i].aggregate()));
    //                 }
    //             }
    //             System.out.println(rs);
    //             versionAfterEndTimeExisted = true;
    //         }
    //     }
        
    //     lastV = currentV;
        
    //     eTime = System.currentTimeMillis();
    //     sumTime += (eTime - sTime);
    // }
    
    // private final void collectDeltaValues(OrcStruct shuffleDeltaValues) {
    //     for (int i = 0; i < aggregatorNames.length; ++i) {
    //         switch (aggregatorNames[i].toString().toLowerCase()) {
    //             case "sum":
    //                 Object deltaValue = TypeUtil.hadoopObj2JObj(
    //                     shuffleDeltaValues.getFieldValue(i)
    //                 );
    //                 aggregators[i].collect(deltaValue, TimelineIndex.STARTIDX);
    //                 break;
                    
    //             case "min":
    //             case "max":
    //                 OrcList becomeValidValueSet = (OrcList)
    //                     ((OrcStruct)shuffleDeltaValues.getFieldValue(i)).getFieldValue(0);
    //                 for (Object value : becomeValidValueSet) {
    //                     aggregators[i].collect(
    //                         TypeUtil.hadoopObj2JObj((WritableComparable)value), 
    //                         TimelineIndex.STARTIDX
    //                     );
    //                 }
    //                 OrcList becomeInvalidValueSet = (OrcList)
    //                     ((OrcStruct)shuffleDeltaValues.getFieldValue(i)).getFieldValue(1);
    //                 for (Object value : becomeInvalidValueSet) {
    //                     aggregators[i].collect(
    //                         TypeUtil.hadoopObj2JObj((WritableComparable)value), 
    //                         TimelineIndex.STARTIDX
    //                     );
    //                 }
    //                 break;
            
    //             default:
    //                 throw new UnsupportedOperationException(
    //                     String.format("Unsupported aggregator %s", aggregatorNames[i].toString())
    //                 );
    //         }
    //     }
    // }
    
    // @Override
    // protected void cleanup(Reducer<OrcKey, OrcValue, NullWritable, NullWritable>.Context context)
    //     throws IOException, InterruptedException 
    // {
    //     Counter counter = context.getCounter("user", "reducer" + reducerId);
    //     counter.setValue(sumTime);
    //     LOG.info("Merge taken {} ms", sumTime);
    // }
}
