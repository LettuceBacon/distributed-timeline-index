package xyz.mfj.dataManipulation.ConvertorImpl;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.mapred.OrcList;

import xyz.mfj.dataManipulation.Convertor;

public abstract class ConverterImpl implements Convertor {
    protected final int offset;
    protected final String nullString;

    ConverterImpl(IntWritable offset, String nullString) {
        this.offset = offset.get();
        offset.set(this.offset + 1);
        this.nullString = nullString;
    }

    @Override
    public void convert(OrcList<Text> values, VectorizedRowBatch batch, int row) {
        convert(values, batch.cols[0], row);
    }
}
