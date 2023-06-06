package xyz.mfj.dataManipulation.ConvertorImpl;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.mapred.OrcList;

public class LongConverter extends ConverterImpl {
    public LongConverter(IntWritable offset, String nullString) {
        super(offset, nullString);
    }

    @Override
    public void convert(OrcList<Text> values, ColumnVector column, int row) {
        String currVal = values.get(offset).toString();
        if (currVal == null || nullString.equals(currVal)) {
            column.noNulls = false;
            column.isNull[row] = true;
        } else {
            ((LongColumnVector) column).vector[row] =
                Long.parseLong(currVal);
        }
    }
}
