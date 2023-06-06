package xyz.mfj.dataManipulation;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.dataDefiniation.Table;

// MR任务执行器，从某种格式文件读取数据，以某种格式存储为某个表的物理存储
public class LoadDataExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(LoadDataExecutor.class);
    
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
    
    public LoadDataExecutor(
        Table table,
        Configuration loadDataJobConf,
        Path inputPath,
        Class<? extends FileInputFormat> inputFormatClass,
        Class<? extends FileOutputFormat> outputFormatClass,
        Class<? extends Mapper> inputMapper,
        Class<? extends Reducer> outputReducer,
        Class<? extends WritableComparable> mapOutputKeyClass,
        Class<? extends Writable> mapOutputValueClass,
        Class<? extends WritableComparable> outputKeyClass,
        Class<? extends Writable> outputValueClass,
        Integer numReduceTasks
    ) {
        this.table = table;
        this.loadDataJobConf = loadDataJobConf;
        this.inputPath = inputPath;
        this.inputFormatClass = inputFormatClass;
        this.outputFormatClass = outputFormatClass;
        this.inputMapperClass = inputMapper;
        this.outputReducerClass = outputReducer;
        this.mapOutputKeyClass = mapOutputKeyClass;
        this.mapOutputValueClass = mapOutputValueClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
        this.numReduceTasks = numReduceTasks;
    }
    
    public void execute() throws IOException, ClassNotFoundException, InterruptedException {
        // 将本轮数据存储位置在表中记录下来
        List<Path> tblStoragePaths = table.getStoragePath();
        Path newStoragePath = new Path(table.getTableName() + Long.toString(System.currentTimeMillis()));
        tblStoragePaths.add(newStoragePath);
        
        Job loadDataJob = Job.getInstance(loadDataJobConf, "loadData");
        loadDataJob.setJarByClass(this.getClass());
        if (inputFormatClass != null) {
            loadDataJob.setInputFormatClass(inputFormatClass);
        }
        if (outputFormatClass != null) {
            loadDataJob.setOutputFormatClass(outputFormatClass);
        }
        loadDataJob.setMapperClass(inputMapperClass);
        loadDataJob.setReducerClass(outputReducerClass);
        loadDataJob.setMapOutputKeyClass(mapOutputKeyClass);
        loadDataJob.setMapOutputValueClass(mapOutputValueClass);
        loadDataJob.setOutputKeyClass(outputKeyClass);
        loadDataJob.setOutputValueClass(outputValueClass);
        if (numReduceTasks != null) {
            loadDataJob.setNumReduceTasks(numReduceTasks);
        }
        FileInputFormat.addInputPath(loadDataJob, inputPath);
        FileOutputFormat.setOutputPath(loadDataJob, newStoragePath);
        
        long sTime = System.currentTimeMillis();
        loadDataJob.waitForCompletion(true);
        // 删除除数据文件之外的文件
        // FileSystem fs = FileSystem.get(loadDataJobConf);
        // RemoteIterator<LocatedFileStatus> fileIter = fs.listFiles(newStoragePath, false);
        // while (fileIter.hasNext()) {
        //     Path filePath = fileIter.next().getPath();
        //     if (!filePath.getName().endsWith(".orc")) {
        //         fs.delete(filePath, true);
        //     }
        // }
        
        long eTime = System.currentTimeMillis();
        LOG.info("Construct table and Timeline index cost {} ms!", eTime - sTime);
    }
}
