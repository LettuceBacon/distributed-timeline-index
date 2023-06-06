package xyz.mfj.dataManipulation.ConvertorImpl;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.mapred.OrcList;

public class DecimalConverter extends ConverterImpl {
    public DecimalConverter(IntWritable offset, String nullString) {
        super(offset, nullString);
    }

    @Override
    public void convert(OrcList<Text> values, ColumnVector column, int row) {
        String currVal = values.get(offset).toString();
        if (currVal == null || nullString.equals(currVal)) {
            column.noNulls = false;
            column.isNull[row] = true;
        } else {
            ((DecimalColumnVector) column).vector[row].set(
                new HiveDecimalWritable(currVal));
        }
    }
}
