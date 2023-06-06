package xyz.mfj.dataManipulation;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.OrcConf;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.Library;
import xyz.mfj.DtlConf;
import xyz.mfj.dataDefiniation.ApplicationPeriod;
import xyz.mfj.dataDefiniation.OrcTable;
import xyz.mfj.dataDefiniation.Table;
import xyz.mfj.utils.SerDeUtil;

// 负责构建LoadDataExecutor
public class LoadDataExecutorBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(CsvInputMapper.class);
    
    private Table table;
    private Configuration loadDataJobConf;
    private Path inputPath;
    private Class<? extends FileInputFormat> inputFormatClass;
    private Class<? extends FileOutputFormat> outputFormatClass;
    private Class<? extends Mapper> inputMapperClass;
    private Class<? extends Reducer> outputReducerClass;
    private Class<? extends WritableComparable> mapOutputKeyClass;
    private Class<? extends Writable> mapOutputValueClass;
    private Class<? extends WritableComparable> outputKeyClass;
    private Class<? extends Writable> outputValueClass;
    private Integer numReduceTasks;
    
    public LoadDataExecutorBuilder() {}
    
    public LoadDataExecutorBuilder intoTable(String tableName) {
        this.table = Library.getInstance().getTableByName(tableName);
        if (this.table == null) {
            LOG.error("There doesn't exist a table {}", tableName);
            System.exit(1);
        }
        
        this.loadDataJobConf = new Configuration(this.table.getConf());
        // 将表内模式作为MR任务输出的模式
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(
            loadDataJobConf, this.table.getInnerSchema().toString()
        );
        // 使用MAPRED_INPUT_SCHEMA存储表的模式，用于解析输入数据
        OrcConf.MAPRED_INPUT_SCHEMA.setString(
            loadDataJobConf, this.table.getTableSchema().toString()
        );
        LOG.info("Table inner schema {}", OrcConf.MAPRED_OUTPUT_SCHEMA.getString(loadDataJobConf));
        
        String fileType = this.table.getStorageFileType();
        switch (fileType.toLowerCase()) {
            case "temporalorc":
                this.outputFormatClass = OrcOutputFormat.class;
                this.outputReducerClass = TemporalOrcOutputReducer.class;
                this.outputKeyClass = NullWritable.class;
                this.outputValueClass = OrcStruct.class;
                // 如果是时态表，将表的有效时间信息设置在conf里
                
                OrcTable orcTable = (OrcTable)table;
                List<ApplicationPeriod> appPrds = new ArrayList<>(
                    orcTable.getAppPrds().values()
                );
                loadDataJobConf.set(DtlConf.APPLICATION_PERIOD, SerDeUtil.serialize(appPrds));
                
                break;

            default:
                LOG.error("Unhandled file type {}", fileType);
                System.exit(1);
                break;
        }
        
        return this;
    }
    
    public LoadDataExecutorBuilder fromPath(String inputName, String fileType) {
        inputPath = new Path(inputName);
        switch (fileType.toLowerCase()) {
            case "csv":
                this.inputFormatClass = TextInputFormat.class;
                this.inputMapperClass = CsvInputMapper.class;
                this.mapOutputKeyClass = IntWritable.class;
                this.mapOutputValueClass = OrcValue.class; // OrcList<Text>
                OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.setString(loadDataJobConf, "array<string>");
                break;
        
            default:
                LOG.error("Unhandled file type {}", fileType);
                System.exit(1);
                break;
        }
        
        return this;
    }
    
    /**
     * 设置job需要的conf，包括csvParser需要的东西
     * @param name
     * @param value
     * @return
     */
    public LoadDataExecutorBuilder withProperties(String name, String value) {
        loadDataJobConf.set(name, value);
        return this;
    }
    
    public LoadDataExecutorBuilder withParallelNum(int numReduceTasks) {
        if (numReduceTasks <= 0) {
            LOG.debug("Load data job use default reduce tasks number");
            this.numReduceTasks = null;
        }
        else {
            this.numReduceTasks = numReduceTasks;
        }
        return this;
    }
    
    public LoadDataExecutor build() {
        String maxTaskMem = loadDataJobConf.get(DtlConf.MAX_TASK_MEM);
        if (maxTaskMem != null) {
            loadDataJobConf.set("mapreduce.reduce.java.opts", "-Xmx" + maxTaskMem);
        }
        return new LoadDataExecutor(
            table, 
            loadDataJobConf, 
            inputPath, 
            inputFormatClass,
            outputFormatClass,
            inputMapperClass,
            outputReducerClass,
            mapOutputKeyClass,
            mapOutputValueClass,
            outputKeyClass,
            outputValueClass,
            numReduceTasks
        );
    }
}
