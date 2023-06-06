package xyz.mfj.dataQuery;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

import xyz.mfj.enhanced.EnhancedVectorizedRowBatch;
import xyz.mfj.readers.EnhancedOrcReader;
import xyz.mfj.readers.StripeReader;
import xyz.mfj.readers.StripeReaderImpl;

public class TemporalOrcStripeReader 
    extends RecordReader<NullWritable, EnhancedVectorizedRowBatch>
{
    private StripeReader stripeReader;
    private TypeDescription schema;
    private EnhancedVectorizedRowBatch rowBatch;
    
    public TemporalOrcStripeReader(EnhancedOrcReader reader, 
        Reader.Options options,
        int stripeRowCount
    ) throws IOException 
    {
        stripeReader = new StripeReaderImpl(reader, options);
        if (options.getSchema() == null) {
            schema = reader.getSchema();
        } else {
            schema = options.getSchema();
        }
        rowBatch = EnhancedVectorizedRowBatch.createRowBatch(stripeRowCount, schema);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) 
        throws IOException, InterruptedException 
    {
        // 什么也不做
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        rowBatch.reset();
        return stripeReader.nextBatch(rowBatch);
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public EnhancedVectorizedRowBatch getCurrentValue() throws IOException, InterruptedException {
        return rowBatch;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        stripeReader.close();
    }
    
}
