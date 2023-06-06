package xyz.mfj.dataManipulation.ConvertorImpl;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;

import xyz.mfj.dataManipulation.Convertor;

public class StructConverter implements Convertor {
    final Convertor[] children;


    public StructConverter(
        IntWritable offset, 
        TypeDescription schema, 
        String nullString, 
        String timestampFormat
    ) {
        children = new Convertor[schema.getChildren().size()];
        int c = 0;
        for(TypeDescription child: schema.getChildren()) {
            children[c++] = Convertor.buildConverter(offset, child, nullString, timestampFormat);
        }
    }

    @Override
    public void convert(OrcList<Text> values, VectorizedRowBatch batch, int row) {
        for(int c=0; c < children.length; ++c) {
            children[c].convert(values, batch.cols[c], row);
        }
    }

    @Override
    public void convert(OrcList<Text> values, ColumnVector column, int row) {
        StructColumnVector cv = (StructColumnVector) column;
        for(int c=0; c < children.length; ++c) {
            children[c].convert(values, cv.fields[c], row);
        }
    }
    
    // @Override
    // public String toString() {
    //     StringBuilder sb = new StringBuilder();
    //     ConverterImpl[] childrenConv = (ConverterImpl[])children;
    //     sb.append(this.getClass().getSimpleName());
    //     sb.append("<");
    //     for (ConverterImpl conv : childrenConv) {
    //         sb.append(conv.offset);
    //         sb.append(':');
    //         sb.append(conv.getClass().getSimpleName());
    //         sb.append(',');
    //     }
    //     sb.append(">");
    //     return sb.toString();
    // }
}
