package xyz.mfj.dataQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;
import xyz.mfj.readers.EnhancedOrcReader;
import xyz.mfj.utils.SerDeUtil;

// TemporalOrcInputFormat的RecordReader会将一个stripe读入一个rowBatch结构存储起来
// 调用getkey时返回nullWritable，调用getValue时返回按时间顺序读到的下一个元组
// 调用nextValue时返回该stripe是否读取完毕
// 输入的键值对<NullWritable, OrcStruct>
// 
public class TemporalOrcInputFormat extends FileInputFormat<NullWritable, EnhancedVectorizedRowBatch> {

    @Override
    public RecordReader<NullWritable, EnhancedVectorizedRowBatch> createRecordReader(
        InputSplit split, TaskAttemptContext context
    ) throws IOException, InterruptedException 
    {
        Configuration conf = context.getConfiguration();
        FileSplit fileSplit = (FileSplit) split;
        long fileStart = fileSplit.getStart();
        long fileLength = fileSplit.getLength();
        EnhancedOrcReader reader = new EnhancedOrcReader(
            fileSplit.getPath(),
            OrcFile.readerOptions(conf)
                .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf))
        );
        TypeDescription innerSchema =
            TypeDescription.fromString(OrcConf.MAPRED_INPUT_SCHEMA.getString(conf));
        innerSchema.getId();
        boolean[] includedCols = SerDeUtil.deserialize(
            OrcConf.INCLUDE_COLUMNS.getString(conf), boolean[].class
        );
        SearchArgument stripeFilter = SerDeUtil.deserialize(
            OrcConf.KRYO_SARG.getString(conf), SearchArgumentImpl.class);
        Reader.Options options = reader.options()
            .range(fileStart, fileLength)
            .useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf))
            .skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf))
            .tolerateMissingSchema(OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf))
            .schema(innerSchema)
            .include(includedCols)
            .searchArgument(stripeFilter, null);
        
        return new TemporalOrcStripeReader(
            reader, 
            options, 
            OrcConf.STRIPE_ROW_COUNT.getInt(conf)
        );
    }

    /**
     * Filter out the 0 byte files, so that we don't generate splits for the
     * empty ORC files.
     * @param job the job configuration
     * @return a list of files that need to be read
     * @throws IOException
     */
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> result = super.listStatus(job);
        List<FileStatus> ok = new ArrayList<>(result.size());
        for(FileStatus stat: result) {
            if (stat.getLen() != 0) {
                ok.add(stat);
            }
        }
        return ok;
    }
    
}
