package xyz.mfj.enhanced;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.RowBatchVersion;
import org.apache.orc.impl.TypeUtils;

import xyz.mfj.enhanced.ColumnVectorEnhance.ColumnVectorCopier;

// 实现 WritableComparable 接口，用来将reader读到条带直接传入mapper里
public class EnhancedVectorizedRowBatch extends VectorizedRowBatch
    implements WritableComparable<EnhancedVectorizedRowBatch> 
{
    public static final int INCREMENT_SIZE = 5000;
    
    public static void appendTo(VectorizedRowBatch src, 
        VectorizedRowBatch dest,
        TypeDescription schema
    ) {
        if (dest == null) {
            throw new NullPointerException("Destination row batch of process \"append\" is null!");
        }
        if (src == null) {
            return;
        }
        
        ColumnVectorCopier[] copiers = ColumnVectorEnhance.createCopiers(schema);
        for (int i = 0; i < src.numCols; ++i) {
            copiers[i].deepCopy(src.cols[i], 0, dest.cols[i], dest.size, src.size);
        }
        dest.size += src.size;
    }
    
    public static EnhancedVectorizedRowBatch createRowBatch(
        int maxSize, TypeDescription schema
    ) {
        EnhancedVectorizedRowBatch rowBatch;
        if (schema.getCategory() == TypeDescription.Category.STRUCT) {
            List<TypeDescription> children = schema.getChildren();
            rowBatch = new EnhancedVectorizedRowBatch(children.size(), maxSize);
            for(int i = 0; i < rowBatch.cols.length; ++i) {
                rowBatch.cols[i] = TypeUtils.createColumn(
                    children.get(i), 
                    RowBatchVersion.ORIGINAL, 
                    maxSize
                );
            }
        }
        else {
            rowBatch = new EnhancedVectorizedRowBatch(1, maxSize);
            rowBatch.cols[0] = TypeUtils.createColumn(schema, RowBatchVersion.ORIGINAL, maxSize);
        }
        rowBatch.reset();
        return rowBatch;
    }
        
    public static EnhancedVectorizedRowBatch createRowBatch(TypeDescription schema) {
        return createRowBatch(INCREMENT_SIZE, schema);
    }
    
    public EnhancedVectorizedRowBatch(int numCols, int size) {
        super(numCols, size);
    }
    
    /**
     * 增大rowBatch到指定大小。设置preserveData为true可以让数据保存下来。
     * @param rowBatch
     * @param newSize
     * @param preserveData
     */
    public void ensureSize(int newSize, boolean preserveData) {
        for (ColumnVector col : cols) {
            col.ensureSize(newSize, preserveData);
        }
        updateSelected(newSize);
    }
    
    public String toString() {
        if (size == 0) {
            return "";
        }
        StringBuilder b = new StringBuilder();

        if (selectedInUse) {
            for (int j = 0; j < size; j++) {
            int i = selected[j];
            b.append('[');
            for (int k = 0; k < projectionSize; k++) {
                int projIndex = projectedColumns[k];
                ColumnVector cv = cols[projIndex];
                if (k > 0) {
                b.append(", ");
                }
                if (cv != null) {
                try {
                    cv.stringifyValue(b, i);
                } catch (Exception ex) {
                    b.append("<invalid>");
                }
                }
            }
            b.append(']');
            if (j < size - 1) {
                b.append('\n');
            }
            }
        } else {
            for (int i = 0; i < size; i++) {
            b.append("| ");
            for (int k = 0; k < projectionSize; k++) {
                int projIndex = projectedColumns[k];
                ColumnVector cv = cols[projIndex];
                if (k > 0) {
                b.append(" | ");
                }
                if (cv != null) {
                try {
                    if (cv instanceof DateColumnVector) {
                        b.append(((DateColumnVector)cv).formatDate(i));
                    }
                    else {
                        cv.stringifyValue(b, i);
                    }
                } catch (Exception ex) {
                    b.append("<invalid>");
                }
                }
            }
            b.append(" |");
            if (i < size - 1) {
                b.append('\n');
            }
            }
        }
        return b.toString();
    }
    

    @Override
    public void write(DataOutput out) throws IOException {
        // 多态类的writable接口太难实现，需要实现每个子类的writable接口
        throw new UnsupportedOperationException("Unsupported method 'write'");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException("Unsupported method 'readFields'");
    }

    @Override
    public int compareTo(EnhancedVectorizedRowBatch arg0) {
        throw new UnsupportedOperationException("Unsupported method 'compareTo'");
    }
}
