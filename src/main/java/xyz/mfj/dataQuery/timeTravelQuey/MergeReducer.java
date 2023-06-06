package xyz.mfj.dataQuery.timeTravelQuey;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.DtlConf;
import xyz.mfj.dataDefiniation.TimelineIndex;
import xyz.mfj.dataQuery.Aggregator;
import xyz.mfj.dataQuery.TemporalMaxAggregator;
import xyz.mfj.dataQuery.TemporalMinAggregator;
import xyz.mfj.dataQuery.TemporalSumAggregator;
import xyz.mfj.utils.TypeUtil;

// key是OrcKey包裹的时间类型OrcTimestamp或DateWritable
// value是OrcValue包裹的OrcStruct，OrcStruct是一行结果
// 暂时不与grouping兼容
public class MergeReducer extends Reducer<OrcKey, OrcValue, NullWritable, NullWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(MergeReducer.class);
    
    private TypeDescription resultSchema;
    private Aggregator[] aggregators;
    
    @Override
    protected void setup(Reducer<OrcKey, OrcValue, NullWritable, NullWritable>.Context context)
        throws IOException, InterruptedException 
    {
        Configuration conf = context.getConfiguration();
        resultSchema = TypeDescription.fromString(OrcConf.MAPRED_OUTPUT_SCHEMA.getString(conf));
        resultSchema.getId();
        aggregators = restoreAggregators(conf);
    }
    
    @Override
    protected void reduce(OrcKey key, Iterable<OrcValue> values,
            Reducer<OrcKey, OrcValue, NullWritable, NullWritable>.Context context
    ) throws IOException, InterruptedException 
    {
        long sTime = System.currentTimeMillis();
        
        int rowCount = 0;
        System.out.println(resultSchema);
        if (aggregators == null) {
            for (OrcValue v : values) {
                System.out.println((OrcStruct)v.value);
                rowCount++;
            }
        }
        else {
            for (OrcValue v : values) {
                OrcStruct row = (OrcStruct)v.value;
                for (int i = 0; i < aggregators.length; ++i) {
                    aggregators[i].collect(
                        TypeUtil.hadoopObj2JObj(row.getFieldValue(i)), 
                        TimelineIndex.STARTIDX
                    );
                }
            }
            OrcStruct resultRow = (OrcStruct) OrcStruct.createValue(resultSchema);
            for (int i = 0; i < aggregators.length; ++i) {
                resultRow.setFieldValue(i, TypeUtil.jObj2hadoopObj(aggregators[i].aggregate()));
            }
            System.out.println(resultRow);
            rowCount++;
        }
        
        long eTime = System.currentTimeMillis();
        LOG.info("Merge taken {} ms\nRow count {} row(s)", eTime - sTime, rowCount);
    }
    
    private Aggregator[] restoreAggregators(Configuration conf) throws IOException {
        Aggregator[] aggregators = null;
        Text[] tempAggregatorNames = null;
        List<TypeDescription> children = resultSchema.getChildren();
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
}
