package xyz.mfj.dataManipulation.ConvertorImpl;

import java.time.LocalDate;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.mapred.OrcList;

public class DateColumnConverter extends ConverterImpl {
    public DateColumnConverter(IntWritable offset, String nullString) { 
        super(offset, nullString); 
    }

    @Override
    public void convert(OrcList<Text> values, ColumnVector column, int row) {
        String currVal = values.get(offset).toString();
        if (currVal == null || nullString.equals(currVal)) {
            column.noNulls = false;
            column.isNull[row] = true;
        } else {
            DateColumnVector vector = (DateColumnVector) column;

            final LocalDate dt = LocalDate.parse(currVal);
            if (dt != null) {
                vector.vector[row] = dt.toEpochDay();
            } else {
                column.noNulls = false;
                column.isNull[row] = true;
            }
        }
    }
}
