package xyz.mfj.dataQuery.timeTravelQuey;

import java.util.List;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcTimestamp;
import org.apache.orc.mapred.OrcValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.DtlConf;
import xyz.mfj.dataDefiniation.ApplicationPeriod;
import xyz.mfj.dataDefiniation.RowVector;
import xyz.mfj.dataDefiniation.TimelineIndex;
import xyz.mfj.dataDefiniation.TimelineIndex.VRF;
import xyz.mfj.dataQuery.Aggregator;
import xyz.mfj.dataQuery.DtlExpression;
import xyz.mfj.dataQuery.TemporalMaxAggregator;
import xyz.mfj.dataQuery.TemporalMinAggregator;
import xyz.mfj.dataQuery.TemporalSumAggregator;
import xyz.mfj.enhanced.ColumnVectorEnhance;
import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;
import xyz.mfj.enhanced.ColumnVectorEnhance.ElemGetter;
import xyz.mfj.utils.SerDeUtil;
import xyz.mfj.utils.TypeUtil;

public class PartialComputeMapper 
    extends Mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue> 
{
    private static final Logger LOG = LoggerFactory.getLogger(PartialComputeMapper.class);
    
    private ApplicationPeriod appPrd;
    private TypeDescription innerSchema;
    private Object containsTime;
    private DtlExpression whereExpression;
    private DtlExpression[] selectExpressions;
    private Aggregator[] aggregators;
    private List<Integer> includedColIds;
    
    private TypeDescription partRsSchm;
    private OrcStruct partRsRow;
    private OrcValue outputValue;
    private OrcKey outputKey;
    
    @Override
    protected void setup(
        Mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue>.Context context
    ) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        appPrd = (ApplicationPeriod)SerDeUtil.deserialize(
            conf.get(DtlConf.APPLICATION_PERIOD), ArrayList.class
        ).get(0);
        innerSchema = TypeDescription.fromString(OrcConf.MAPRED_INPUT_SCHEMA.getString(conf));
        innerSchema.getId();
        
        containsTime = restoreContainsTime(conf);
        
        if (conf.get(DtlConf.WHERE_EXPR) != null) {
            whereExpression = DefaultStringifier.load(
                conf, DtlConf.WHERE_EXPR, DtlExpression.class
            );
        } // else whereExpression = null
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
        
        partRsRow = (OrcStruct)OrcStruct.createValue(partRsSchm);
        outputValue = new OrcValue(partRsRow);
        outputKey = new OrcKey(TypeUtil.jObj2hadoopObj(containsTime));
        
    }
    
    private Object restoreContainsTime(Configuration conf) throws IOException {
        String containsTimeType = conf.get(DtlConf.PERIOD_ENDPOINT_TYPE);
        if (containsTimeType.equals("timestamp")) {
            return Timestamp.valueOf(
                conf.get(DtlConf.CONTAINS_TIME)
            );
        }
        else {
            return LocalDate.parse(
                conf.get(DtlConf.CONTAINS_TIME)
            );
        }
    }
    
    private Aggregator[] restoreAggregators(Configuration conf) throws IOException {
        Aggregator[] aggregators = null;
        Text[] tempAggregatorNames = null;
        List<TypeDescription> children = partRsSchm.getChildren();
        if (conf.get(DtlConf.SELECT_TEMPORAL_AGGS) != null) {
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
        }
        return aggregators;
    }
    
    @Override
    protected void map(NullWritable key, EnhancedVectorizedRowBatch stripeRowBatch,
        Mapper<NullWritable, EnhancedVectorizedRowBatch, OrcKey, OrcValue>.Context context
    ) throws IOException, InterruptedException {
        long sTime = System.currentTimeMillis();
        long eTime = 0L;
        
        BitSet validRows = new BitSet(stripeRowBatch.size);
        
        LOG.info("Checkpoint is not supported at this time, start linear scan from the beginning of index");
        // 先顺序读checkpoints，找到起始时间
        // 没有checkpoints，起始时间为versionMap第一个时间

        // 遍历timeline，生成containsTime上的有效行
        Iterator<VRF> iter = appPrd.getIndex().iterator(
            appPrd, stripeRowBatch, innerSchema
        );
        VRF vrf = null;
        while (iter.hasNext()) {
            vrf = iter.next();
            if (vrf.getVersion().compareTo(containsTime) > 0) {
                // 当v比containsTime大时
                break;
            }
            // 如果flag与TimelineIndex.STARTIDX相等，即表示vrf是一个有效时间起始时间，设rowId为有效行，
            // 即Bitset.set(rowId, true)；否则设rowId为无效，即Bitset.set(rowId, false)。
            validRows.set(vrf.getRowId(), vrf.getFlag() == TimelineIndex.STARTIDX);
        }
        iter = null;
        
        // 遍历每一个有效行，应用where语句筛选，满足条件的应用select语句映射到结果表中
        int[] validRowIds = validRows.stream().toArray();
        ElemGetter[] stripeRowBatchGetters = 
            ColumnVectorEnhance.createElemGetters(innerSchema);
        boolean hasAggregator = aggregators != null;
        int includedColNum = includedColIds.size();
        for (int i = 0; i < validRowIds.length; i++) {
            RowVector row = new RowVector(stripeRowBatchGetters.length);
            for (int j = 0; j < includedColNum; ++j) {
                int includedColId = includedColIds.get(j);
                row.set(
                    includedColId, 
                    // stripeRowBatchGetters[includedColId].getHadoopElem(
                    stripeRowBatchGetters[includedColId].getElem(
                        stripeRowBatch.cols[includedColId], validRowIds[i]
                    )
                );
            }
            
            if (whereExpression != null) {
                Object evalVal = whereExpression.evaluate(row);
                if (evalVal == null || !(boolean)evalVal) {
                    // validRows.set(validRowIds[i], false);
                    row = null;
                }
            }
            if (row == null) continue; // 2

            if (hasAggregator) { // 如果有聚合函数，计算并收集数据
                for (int j = 0; j < selectExpressions.length; j++) {
                    aggregators[j].collect(
                        selectExpressions[j].evaluate(row), TimelineIndex.STARTIDX
                    );
                }
            }
            else { // 没有聚合函数，计算后直接生成一行结果，结果中只有Select列
                for (int j = 0; j < selectExpressions.length; ++j) {
                    // 给select列赋值
                    partRsRow.setFieldValue(j, 
                        TypeUtil.jObj2hadoopObj(selectExpressions[j].evaluate(row))
                    );
                    
                }
                context.write(outputKey, outputValue);
            }
        }
        // 执行聚合
        if (hasAggregator) {
            for (int j = 0; j < aggregators.length; ++j) {
                // 给select列赋值
                partRsRow.setFieldValue(j, 
                    TypeUtil.jObj2hadoopObj(aggregators[j].aggregate())
                );
            }
            outputKey.key = TypeUtil.jObj2hadoopObj(containsTime);
            context.write(outputKey, outputValue);
        }
        
        if (hasAggregator) {
            for (int j = 0; j < aggregators.length; ++j) {
                aggregators[j].reset();
            }
        }
        
        eTime = System.currentTimeMillis();
        LOG.info("Partial compute taken {} ms", eTime - sTime);
    }
}
