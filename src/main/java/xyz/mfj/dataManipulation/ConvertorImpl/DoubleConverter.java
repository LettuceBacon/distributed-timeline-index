package xyz.mfj.dataManipulation.ConvertorImpl;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.mapred.OrcList;

public class DoubleConverter extends ConverterImpl {
    public DoubleConverter(IntWritable offset, String nullString) {
        super(offset, nullString);
    }

    @Override
    public void convert(OrcList<Text> values, ColumnVector column, int row) {
        String currVal = values.get(offset).toString();
        if (currVal == null || nullString.equals(currVal)) {
            column.noNulls = false;
            column.isNull[row] = true;
        } else {
        ((DoubleColumnVector) column).vector[row] =
            Double.parseDouble(currVal);
        }
    }
}
