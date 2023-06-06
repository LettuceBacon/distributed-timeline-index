package xyz.mfj.dataManipulation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.DtlConf;
import xyz.mfj.dataDefiniation.ApplicationPeriod;
import xyz.mfj.enhanced.ColumnVectorEnhance;
import xyz.mfj.enhanced.ColumnVectorEnhance.ElemGetter;
import xyz.mfj.utils.SerDeUtil;

// 每集齐一个条带行数，构建索引，然后写入文件
public class TemporalOrcOutputReducer 
    extends Reducer<IntWritable, OrcValue, NullWritable, OrcStruct> 
{
    private static final Logger LOG = LoggerFactory.getLogger(TemporalOrcOutputReducer.class);
    private static final String NULLSTRING = "null";
    private static final String DEFAULT_TIMESTAMP_FORMAT =
        "yyyy[[-][/]]MM[[-][/]]dd[['T'][ ]]HH:mm:ss[ ][XXX][X]";
    public static final String NULLSTRING_CONF_NAME = "null-string";
    public static final String DEFAULT_TIMESTAMP_FORMAT_CONF_NAME = "default-timestamp-format";
    
    private int rowCountInStripe;
    private String nullString;
    private String timestampFormat;
    private TypeDescription tableInnerSchema;
    private TypeDescription tableSchema;
    private VectorizedRowBatch stripeRowBatch;
    private List<ApplicationPeriod> appPrds;
    private Convertor convertor;
    private OrcStruct tableRow;
    private ElemGetter[] getters;
    private NullWritable keyOut;
    private long sTime;
    private long eTime;
    
    @Override
    protected void setup(Reducer<IntWritable, OrcValue, NullWritable, OrcStruct>.Context context)
            throws IOException, InterruptedException 
    {
        Configuration jobConf = context.getConfiguration();
        rowCountInStripe = OrcConf.STRIPE_ROW_COUNT.getInt(jobConf);
        nullString = jobConf.get(NULLSTRING_CONF_NAME, NULLSTRING);
        timestampFormat = jobConf.get(DEFAULT_TIMESTAMP_FORMAT_CONF_NAME, DEFAULT_TIMESTAMP_FORMAT);
        tableInnerSchema = TypeDescription.fromString(
            OrcConf.MAPRED_OUTPUT_SCHEMA.getString(jobConf)
        );
        tableInnerSchema.getId(); // 初始化id
        tableSchema = TypeDescription.fromString(
            OrcConf.MAPRED_INPUT_SCHEMA.getString(jobConf)
        );
        tableSchema.getId(); // 初始化id
        stripeRowBatch = tableInnerSchema.createRowBatch(rowCountInStripe);
        convertor = Convertor.buildConverter(
            new IntWritable(0), tableSchema, nullString, timestampFormat
        );
        appPrds = SerDeUtil.deserialize(
            jobConf.get(DtlConf.APPLICATION_PERIOD), ArrayList.class
        );
        tableRow = (OrcStruct)OrcStruct.createValue(tableInnerSchema);
        getters = ColumnVectorEnhance.createElemGetters(tableInnerSchema);
        keyOut = NullWritable.get();
        LOG.debug("rowCountInStripe {} tableInnerSchema {}", 
            rowCountInStripe,
            tableInnerSchema.toString());
        sTime = System.currentTimeMillis();
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<OrcValue> values,
            Reducer<IntWritable, OrcValue, NullWritable, OrcStruct>.Context context)
            throws IOException, InterruptedException 
    {
        for (OrcValue value : values) {
            // 从一行字面值解析为数值，加入到stripeRowBatch中
            OrcList<Text> row = (OrcList<Text>)value.value;
            convertor.convert(row, stripeRowBatch, stripeRowBatch.size);
            stripeRowBatch.size++;
            
            if (stripeRowBatch.size >= rowCountInStripe) {
                // 构造索引
                for (ApplicationPeriod appPrd : appPrds) {
                    appPrd.getIndex().buildIndex(stripeRowBatch, tableInnerSchema, appPrd);
                    
                } 
                
                // 逐行包装成OrcStruct，写入文件
                for (int i = 0; i < stripeRowBatch.size; ++i) {
                    for (int j = 0; j < stripeRowBatch.numCols; ++j) {
                        tableRow.setFieldValue(
                            j, 
                            getters[j].getHadoopElem(stripeRowBatch.cols[j], i) // i行j列
                        );
                    }
                    context.write(keyOut, tableRow);
                }
                stripeRowBatch.reset();
            }
        }
    }
    
    
    @Override
    protected void cleanup(Reducer<IntWritable, OrcValue, NullWritable, OrcStruct>.Context context)
            throws IOException, InterruptedException 
    {
        // 将不足一个条带的写入文件
        if (stripeRowBatch.size > 0) {
            // 构造索引
            for (ApplicationPeriod appPrd : appPrds) {
                appPrd.getIndex().buildIndex(stripeRowBatch, tableInnerSchema, appPrd);
            }
            
            // 逐行包装成OrcStruct，写入文件
            for (int i = 0; i < stripeRowBatch.size; ++i) {
                for (int j = 0; j < stripeRowBatch.numCols; ++j) {
                    tableRow.setFieldValue(
                        j, 
                        getters[j].getHadoopElem(stripeRowBatch.cols[j], i) // i行j列
                    );
                }
                context.write(keyOut, tableRow);
            }
            stripeRowBatch.reset();
        }
                
        eTime = System.currentTimeMillis();
        LOG.info("Reducer {} cost {} ms",
            context.getTaskAttemptID().getTaskID().getId(),
            eTime - sTime);
    }
}
