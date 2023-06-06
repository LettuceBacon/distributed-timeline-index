package xyz.mfj.dataManipulation;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;

import xyz.mfj.dataManipulation.ConvertorImpl.BooleanConverter;
import xyz.mfj.dataManipulation.ConvertorImpl.BytesConverter;
import xyz.mfj.dataManipulation.ConvertorImpl.DateColumnConverter;
import xyz.mfj.dataManipulation.ConvertorImpl.DecimalConverter;
import xyz.mfj.dataManipulation.ConvertorImpl.DoubleConverter;
import xyz.mfj.dataManipulation.ConvertorImpl.LongConverter;
import xyz.mfj.dataManipulation.ConvertorImpl.StructConverter;
import xyz.mfj.dataManipulation.ConvertorImpl.TimestampConverter;

public interface Convertor {
    public void convert(OrcList<Text> values, VectorizedRowBatch batch, int row);
    public void convert(OrcList<Text> values, ColumnVector column, int row);
    
    public static Convertor buildConverter(
        IntWritable startOffset, 
        TypeDescription schema,
        String nullString,
        String timestampFormat
    ) {
        switch (schema.getCategory()) {
            case BOOLEAN:
                return new BooleanConverter(startOffset, nullString);
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new LongConverter(startOffset, nullString);
            case FLOAT:
            case DOUBLE:
                return new DoubleConverter(startOffset, nullString);
            case DECIMAL:
                return new DecimalConverter(startOffset, nullString);
            case BINARY:
            case STRING:
            case CHAR:
            case VARCHAR:
                return new BytesConverter(startOffset, nullString);
            case DATE:
                return new DateColumnConverter(startOffset, nullString);
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return new TimestampConverter(startOffset, nullString, timestampFormat);
            case STRUCT:
                return new StructConverter(startOffset, schema, nullString, timestampFormat);
            default:
                throw new IllegalArgumentException("Unhandled type " + schema);
        }
    }
}
