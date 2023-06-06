package xyz.mfj.dataQuery.temporalGroupingQuery;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.curator.framework.api.ParentACLable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.DtlConf;
import xyz.mfj.dataDefiniation.ApplicationPeriod;
import xyz.mfj.dataDefiniation.RowVector;
import xyz.mfj.dataDefiniation.TimelineIndex;
import xyz.mfj.dataDefiniation.TimelineIndex.VRF;
import xyz.mfj.dataQuery.Aggregator;
import xyz.mfj.dataQuery.DeltaStruct;
import xyz.mfj.dataQuery.DtlExpression;
import xyz.mfj.dataQuery.TemporalMaxAggregator;
import xyz.mfj.dataQuery.TemporalMinAggregator;
import xyz.mfj.dataQuery.TemporalSumAggregator;
import xyz.mfj.enhanced.ColumnVectorEnhance;
import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;
import xyz.mfj.enhanced.ColumnVectorEnhance.ElemGetter;
import xyz.mfj.utils.SerDeUtil;
import xyz.mfj.utils.TypeUtil;

// outputkey是时间窗口号，outputvalue是select列+mapper号+mapper读取的条带序号
public class PartialComputeMapper 
    extends Mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue> 
{
    private static final Logger LOG = LoggerFactory.getLogger(PartialComputeMapper.class);
    
    // Distributed Timeline
    private ApplicationPeriod appPrd;
    private TypeDescription innerSchema;
    private Comparable startTime;
    private Comparable endTime;
    private DtlExpression whereExpression;
    private DtlExpression[] selectExpressions;
    private Aggregator[] aggregators;
    private List<Integer> includedColIds;
    private long windowLen;
    private Comparable windowMargin;
    // 如果在一个时间点上，有满足where条件的行数据或者没有where条件，这个标记变为真
    private boolean validRowExisted;
    
    private int mapperId;
    private int stripeId;
    private TypeDescription partRsSchm;
    private OrcStruct partRsRow;
    private OrcValue outputValue;
    private OrcKey outputKey;
    private long sTime;
    private long eTime;
    private long sumTime;
    
    @Override
    protected void setup(Mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue>.Context context)
        throws IOException, InterruptedException 
    {
        Configuration conf = context.getConfiguration();
        appPrd = (ApplicationPeriod)SerDeUtil.deserialize(
            conf.get(DtlConf.APPLICATION_PERIOD), ArrayList.class
        ).get(0);
        innerSchema = TypeDescription.fromString(OrcConf.MAPRED_INPUT_SCHEMA.getString(conf));
        innerSchema.getId();
        
        restoreOverlapsTime(conf);
        
        if (conf.get(DtlConf.WHERE_EXPR) != null) {
            whereExpression = DefaultStringifier.load(
                conf, DtlConf.WHERE_EXPR, DtlExpression.class
            );
        } 
        // else whereExpression = null
        selectExpressions = DefaultStringifier.loadArray(conf, DtlConf.SELECT_EXPR, DtlExpression.class);
        
        partRsSchm = TypeDescription.fromString(OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.getString(conf));
        partRsSchm.getId();
        
        aggregators = restoreAggregators(conf);
        
        boolean[] includedCols = SerDeUtil.deserialize(
            OrcConf.INCLUDE_COLUMNS.getString(conf), boolean[].class
        );
        includedColIds = new ArrayList<>();
        for (int i = 1; i < includedCols.length; ++i) {
            if (includedCols[i] == true) {
                includedColIds.add(i - 1);
            }
        }
        
        // 如果没有给WINDOW_LENGTH赋值，就没有窗口，windowMargin始终等于startTime + 0L
        windowLen = conf.getLong(DtlConf.WINDOW_LENGTH, 0L);
        
        validRowExisted = false;
        
        mapperId = context.getTaskAttemptID().getTaskID().getId();
        stripeId = 0;
        
        partRsRow = (OrcStruct)OrcStruct.createValue(partRsSchm);
        outputValue = new OrcValue(partRsRow);
        outputKey = new OrcKey();
        sumTime = 0L;
    }
    
    private void restoreOverlapsTime(Configuration conf) throws IOException {
        String overlapsPeriodEndpointType = conf.get(DtlConf.PERIOD_ENDPOINT_TYPE);
        if (overlapsPeriodEndpointType.equals("timestamp")) {
            startTime = Timestamp.valueOf(
                conf.get(DtlConf.OVERLAPS_START_TIME)
            );
            endTime = Timestamp.valueOf(
                conf.get(DtlConf.OVERLAPS_END_TIME)
            );
        }
        else {
            startTime = LocalDate.parse(
                conf.get(DtlConf.OVERLAPS_START_TIME)
            );
            endTime = LocalDate.parse(
                conf.get(DtlConf.OVERLAPS_END_TIME)
            );
        }
    }
    
    private Aggregator[] restoreAggregators(Configuration conf) throws IOException {
        Aggregator[] aggregators = null;
        Text[] tempAggregatorNames = null;
        List<TypeDescription> children = partRsSchm.getChildren();
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
    protected void map(NullWritable key, EnhancedVectorizedRowBatch stripeRowBatch,
        Mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue>.Context context)
        throws IOException, InterruptedException 
    {
        sTime = System.currentTimeMillis();    
        
        // 首先读到startTime之后的时间点，
        // 然后在每个时间点上聚合，key为这个时间点，value为带有mapper和stripe标识的一行聚合结果
        // 当查询的时间跨度大时，会设置进行多reduce归并，此时部分聚合后的结果可能会被分区器分给多个reducer
        // 分区器会将查询的时间区间均分为reduce个，然后将key在每个时间区间的数据分配给对应的reducer
        // 具体的实现，在部分聚合计算时，需要将时间区间边界之前的时间点上的数据复制一份，并使用时间区间边界作为key，这是因为在时间区间边界上，这个stripe仍然具有有效的聚合数值，在另一个reducer中需要计算，见aggregateOnTime函数
        
        Text mapperIdStripeId = new Text(mapperId + "_" + stripeId);
        BitSet validRows = new BitSet(stripeRowBatch.size);
        
        Iterator<VRF> iter = appPrd.getIndex().iterator(
            appPrd, stripeRowBatch, innerSchema
        );
        VRF vrf = null;
        Comparable currentV = null;
        Comparable nextV = null;
        ElemGetter[] stripeRowBatchGetters = ColumnVectorEnhance.createElemGetters(innerSchema);
        int includedColNum = includedColIds.size();
        
        
        // 遍历timeline，直到startTime之后一个时间版本
        while (iter.hasNext()) {
            vrf = iter.next();
            nextV = vrf.getVersion();
            if (currentV == null || !nextV.equals(currentV)) {
                // 取到一个新时间版本
                if (nextV.compareTo(startTime) > 0) {
                    break;
                }
                currentV = nextV;
            }
            
            // 如果flag与TimelineIndex.STARTIDX相等，即表示vrf是一个有效时间起始时间，设rowId为有效行，
            // 即Bitset.set(rowId, true)；否则设rowId为无效，即Bitset.set(rowId, false)。
            validRows.set(vrf.getRowId(), vrf.getFlag() == TimelineIndex.STARTIDX);
        }
        
        // 根据validRows执行收集，收集所有小于等于startTime的元组
        int[] validRowIds = validRows.stream().toArray();
        for (int i = 0; i < validRowIds.length; i++) {
            collectRow(stripeRowBatch, 
                stripeRowBatchGetters, 
                includedColNum, 
                includedColIds, 
                validRowIds[i],
                TimelineIndex.STARTIDX);
        }
        
        moveWindowMarginAfter(startTime);
        if (currentV != null) {
            // currentV <= startTime && startTime < nextV
            aggregateOnTime(startTime, nextV, context, mapperIdStripeId, windowMargin);
        }
        // 如果current == null，startTime < nextV == firstV，firstV之前没有数据

        if (nextV.compareTo(endTime) >= 0) {
            // nextV >= endTime --> StartTime < endTime <= nextV
            return;
        }
        
        currentV = nextV;
        // 收集刚刚好大于startTime的元组
        collectRow(stripeRowBatch, 
            stripeRowBatchGetters, 
            includedColNum, 
            includedColIds, 
            vrf.getRowId(),
            vrf.getFlag());
        
        while (iter.hasNext()) {
            vrf = iter.next();
            nextV = vrf.getVersion();
            
            if (currentV == null || !nextV.equals(currentV)) {
                // 取到一个新时间版本
                if (nextV.compareTo(endTime) >= 0) {
                    // currentV < endTime <= nextV <= finalV
                    aggregateOnTime(currentV, endTime, context, mapperIdStripeId, windowMargin);
                    break;
                }
                // 否则执行聚合
                aggregateOnTime(currentV, nextV, context, mapperIdStripeId, windowMargin);
                currentV = nextV;
            }
            
            // 收集currentV到nextV上的元组
            collectRow(stripeRowBatch, 
                stripeRowBatchGetters, 
                includedColNum, 
                includedColIds, 
                vrf.getRowId(), 
                vrf.getFlag());
            
            // 如果flag与TimelineIndex.STARTIDX相等，即表示vrf是一个有效时间起始时间，设rowId为有效行，
            // 即Bitset.set(rowId, true)；否则设rowId为无效，即Bitset.set(rowId, false)。
            // validRows.set(vrf.getRowId(), vrf.getFlag() == TimelineIndex.STARTIDX);
            
        }

        // 如果 !iter.hasNext()，currentV == nextV == finalV < endTime，finalV之后没有数据
        
        for (int j = 0; j < aggregators.length; ++j) {
            aggregators[j].reset();
        }
        stripeId++;
        
        eTime = System.currentTimeMillis();
        sumTime += (eTime - sTime);
    }
    
    private final void collectRow(
        VectorizedRowBatch srcRowBatch,
        ElemGetter[] srcGetters,
        int includedColNum,
        List<Integer> includedColIds,
        int rowId,
        int flag
    ) {
        RowVector row = new RowVector(srcGetters.length);
        for (int j = 0; j < includedColNum; ++j) {
            int includedColId = includedColIds.get(j);
            row.set(
                includedColId, 
                // srcGetters[includedColId].getHadoopElem(
                srcGetters[includedColId].getElem(
                    srcRowBatch.cols[includedColId], rowId
                )
            );
        }
        
        if (whereExpression != null) {
            Object evalVal = whereExpression.evaluate(row);
            if (evalVal == null || !(boolean)evalVal) {
                row = null;
                return;
            }
        }
        
        validRowExisted = true;
        

        for (int j = 0; j < selectExpressions.length; j++) {
            aggregators[j].collect(
                selectExpressions[j].evaluate(row), flag
            );
        }
    }
    
    private final void moveWindowMarginAfter(Comparable time) {
        if (time.getClass().equals(Timestamp.class)) {
            windowMargin = new Timestamp(
                ((Timestamp)time).getTime() + windowLen
            );
        }
        else {
            windowMargin = LocalDate.ofEpochDay(
                ((LocalDate)time).toEpochDay() + windowLen
            );
        }
    }
    
    private final void aggregateOnTime(Comparable currentV, Comparable nextV,
        Mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue>.Context context,
        Text mapperIdStripeId, Comparable windowMargin
    ) throws IOException, InterruptedException {
        if (!validRowExisted) {
            if (windowMargin.compareTo(nextV) <= 0) {
                moveWindowMarginAfter(windowMargin);
            }
            return;
        }
        
        outputKey.key = TypeUtil.jObj2hadoopObj(currentV);
        for (int j = 0; j < aggregators.length; ++j) {
            // 给select列赋值
            partRsRow.setFieldValue(j, 
                TypeUtil.jObj2hadoopObj(aggregators[j].aggregate())
            );
        }
        // 给mapperIdStripeId赋值
        partRsRow.setFieldValue(partRsSchm.getMaximumId() - 1, mapperIdStripeId);
        context.write(outputKey, outputValue);
        
        // 如果windowMargin位于curV和nextV之间，将currentV上聚合结果作为下一个时间窗口起始边界的聚合结果
        if (windowMargin.compareTo(currentV) > 0 && windowMargin.compareTo(nextV) < 0) {
            outputKey.key = TypeUtil.jObj2hadoopObj(windowMargin);
            context.write(outputKey, outputValue);
        }
        
        if (windowMargin.compareTo(nextV) <= 0) {
            moveWindowMarginAfter(windowMargin);
        }
        
        // 重置标志
        validRowExisted = false;
    }

    @Override
    protected void cleanup(Mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue>.Context context)
        throws IOException, InterruptedException 
    {
        Counter mapCounter = context.getCounter("user", "mapper" + mapperId);
        mapCounter.setValue(sumTime);
        LOG.info("Partial compute taken {} ms", sumTime);
    }
    
    // // ParTime
    // private ApplicationPeriod appPrd;
    // private TypeDescription innerSchema;
    // private Comparable startTime;
    // private Comparable endTime;
    // private DtlExpression whereExpression;
    // private DtlExpression[] selectExpressions;
    // private Text[] aggregatorNames;
    // private List<Integer> includedColIds;
    
    // private TypeDescription deltaValuesSchema;
    // private OrcStruct shuffleDeltaValues;
    // private OrcValue outputValue;
    // private OrcKey outputKey;
    // private long sTime;
    // private long eTime;
    // private long sumTime;
    // private int mapperId;
    
    // @Override
    // protected void setup(Mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue>.Context context)
    //     throws IOException, InterruptedException 
    // {
    //     Configuration conf = context.getConfiguration();
    //     appPrd = (ApplicationPeriod)SerDeUtil.deserialize(
    //         conf.get(DtlConf.APPLICATION_PERIOD), ArrayList.class
    //     ).get(0);
    //     innerSchema = TypeDescription.fromString(OrcConf.MAPRED_INPUT_SCHEMA.getString(conf));
    //     innerSchema.getId();
        
    //     restoreOverlapsTime(conf);
        
    //     if (conf.get(DtlConf.WHERE_EXPR) != null) {
    //         whereExpression = DefaultStringifier.load(
    //             conf, DtlConf.WHERE_EXPR, DtlExpression.class
    //         );
    //     } 
    //     // else whereExpression = null
    //     selectExpressions = DefaultStringifier.loadArray(conf, DtlConf.SELECT_EXPR, DtlExpression.class);
        
    //     deltaValuesSchema = TypeDescription.fromString(
    //         OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.getString(conf)
    //     );
    //     deltaValuesSchema.getId();
        
    //     aggregatorNames = DefaultStringifier.loadArray(conf, DtlConf.SELECT_TEMPORAL_AGGS, Text.class);
        
    //     boolean[] includedCols = SerDeUtil.deserialize(
    //         OrcConf.INCLUDE_COLUMNS.getString(conf), boolean[].class
    //     );
    //     includedColIds = new ArrayList<>();
    //     for (int i = 1; i < includedCols.length; ++i) {
    //         if (includedCols[i] == true) {
    //             includedColIds.add(i - 1);
    //         }
    //     }
        
    //     shuffleDeltaValues = (OrcStruct)OrcStruct.createValue(deltaValuesSchema);
    //     outputValue = new OrcValue(shuffleDeltaValues);
    //     outputKey = new OrcKey();
        
    //     mapperId = context.getTaskAttemptID().getTaskID().getId();    
    //     sumTime = 0L;
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
    // protected void map(NullWritable key, EnhancedVectorizedRowBatch stripeRowBatch,
    //     Mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue>.Context context)
    //     throws IOException, InterruptedException 
    // {
    //     sTime = System.currentTimeMillis();
        
    //     ElemGetter[] stripeRowBatchGetters = ColumnVectorEnhance.createElemGetters(innerSchema);
    //     int includedColNum = includedColIds.size();
        
    //     TreeMap<Comparable, DeltaStruct[]> deltaMap = new TreeMap<>();
        
    //     for (int rowId = 0; rowId < stripeRowBatch.size; rowId++) {
    //         RowVector record = new RowVector(stripeRowBatchGetters.length);
    //         // 将rowBatch的每一行includedCol的数据放入一个rowVector
    //         for (int j = 0; j < includedColNum; ++j) {
    //             int includedColId = includedColIds.get(j);
    //             record.set(
    //                 includedColId, 
    //                 // srcGetters[includedColId].getHadoopElem(
    //                 stripeRowBatchGetters[includedColId].getElem(
    //                     stripeRowBatch.cols[includedColId], rowId
    //                 )
    //             );
    //         }
            
    //         // 筛选掉不与start和end相交的或者不满足where条件的数据
    //         Comparable recordValidFrom = (Comparable)record.get(appPrd.getAppPrdSId());
    //         Comparable recordValidTo = (Comparable)record.get(appPrd.getAppPrdEId());
    //         if (recordValidTo.compareTo(startTime) <= 0 || recordValidFrom.compareTo(endTime) >= 0) {
    //             // 如果`ValidTo <= start`或者`end <= ValidFrom`，则时间区间不相交
    //             record = null;
    //         }
    //         if (record == null) continue;
            
    //         if (whereExpression != null) {
    //             Object evalVal = whereExpression.evaluate(record);
    //             if (evalVal == null || !(boolean)evalVal) {
    //                 record = null;
    //             }
    //         }
    //         if (record == null) continue;
            
            
    //         generatreDeltaMap(deltaMap, recordValidFrom, TimelineIndex.STARTIDX, record);
    //         generatreDeltaMap(deltaMap, recordValidTo, TimelineIndex.ENDIDX, record);
    //     }
        
    //     for (Comparable version: deltaMap.navigableKeySet()) {
    //         outputKey.key = TypeUtil.jObj2hadoopObj(version);
    //         DeltaStruct[] deltaValues = deltaMap.get(version);
    //         // 将deltaValues转化为hadoop混洗过程需要的形式
    //         for (int i = 0; i < deltaValues.length; ++i) {
    //             deltaValues[i].convertTo(shuffleDeltaValues, i);
    //         }
    //         context.write(outputKey, outputValue);
    //     }
        
    //     eTime = System.currentTimeMillis();
    //     sumTime +=  (eTime - sTime);
        
    // }
    
    // private final void generatreDeltaMap(TreeMap<Comparable, DeltaStruct[]> deltaMap,
    //     Comparable version, 
    //     int flag,
    //     RowVector record
    // ) {
    //     // 从deltaMap中查看是否已存在时间版本
    //     // 如果不存在，根据agg不同构造一个数组，并处理这个数组
    //     // 如果已存在，根据agg不同处理取出的这个数组
        
    //     DeltaStruct[] deltaValues = deltaMap.get(version);
    //     if (deltaValues == null) {
    //         // 
    //         deltaValues = DeltaStruct.createDeltaValues(aggregatorNames, deltaValuesSchema);
    //         deltaMap.put(version, deltaValues);
    //     }
    //     for (int i = 0; i < selectExpressions.length; ++i) {
    //         deltaValues[i].storeDeltaValue(flag, selectExpressions[i].evaluate(record));
    //     }
    // }
    
    // @Override
    // protected void cleanup(Mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue>.Context context)
    //     throws IOException, InterruptedException 
    // {
    //     Counter testCounter = context.getCounter("user", "mapper" + mapperId);
    //     testCounter.setValue(sumTime);
    //     LOG.info("Partial compute taken {} ms", sumTime);
    // }
    
}
