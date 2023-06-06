package xyz.mfj.dataManipulation.ConvertorImpl;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.mapred.OrcList;

public class BytesConverter extends ConverterImpl {
    public BytesConverter(IntWritable offset, String nullString) {
        super(offset, nullString);
    }

    @Override
    public void convert(OrcList<Text> values, ColumnVector column, int row) {
        String currVal = values.get(offset).toString();
        if (currVal == null || nullString.equals(currVal)) {
            column.noNulls = false;
            column.isNull[row] = true;
        } else {
            byte[] value = currVal.getBytes(StandardCharsets.UTF_8);
            ((BytesColumnVector) column).setRef(row, value, 0, value.length);
        }
    }
}
