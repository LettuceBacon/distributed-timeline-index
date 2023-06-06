package xyz.mfj.enhanced;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnVectorEnhance {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnVectorEnhance.class);
    
    /**
     * 创建模式对应的rowBatch每一列的deepCopy方法
     * @param schema
     * @return 
     */
    public static ColumnVectorCopier[] createCopiers(TypeDescription schema) {
        ColumnVectorCopier[] copiers = null;
        if (schema.getCategory().isPrimitive()) {
            copiers = new ColumnVectorCopier[]{ColumnVectorCopier.newInstance(schema)};
        }
        else if (schema.getCategory().equals(TypeDescription.Category.STRUCT)) {
            List<TypeDescription> children = schema.getChildren();
            copiers = new ColumnVectorCopier[children.size()];
            for (int i = 0; i < copiers.length; ++i) {
                copiers[i] = ColumnVectorCopier.newInstance(children.get(i));
            }
        }
        else {
            throw new IllegalArgumentException("Unhandled type " + schema.getCategory());
        }
        
        return copiers;
    }
    
    public static interface ColumnVectorCopier {
        public void deepCopy(ColumnVector srcVec, int srcRow,
            ColumnVector destVec, int destRow,
            int length);
            
        public static ColumnVectorCopier newInstance(TypeDescription typeDesc) {
            switch (typeDesc.getCategory()) {
                case BOOLEAN:
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                  return new LongColumnVectorCopier();
                case DATE:
                    return new DateColumnVectorCopier();
                case TIMESTAMP:
                case TIMESTAMP_INSTANT:
                  return new TimestampColumnVectorCopier();
                case FLOAT:
                case DOUBLE:
                  return new DoubleColumnVectorCopier();
                case DECIMAL:
                    return new DecimalColumnVectorCopier();
                case STRING:
                case BINARY:
                case CHAR:
                case VARCHAR:
                  return new BytesColumnVectorCopier();
                // case STRUCT: {
                //     List<TypeDescription> children = typeDesc.getChildren();
                //     ColumnVectorAppender[] fieldAppenders = new ColumnVectorAppender[children.size()];
                //     for(int i=0; i < fieldAppenders.length; ++i) {
                //         fieldAppenders[i] = ColumnVectorAppender.newInstance(children.get(i));
                //     }
                //     return new StructColumnVectorAppender(fieldAppenders);
                // }
                // case UNION: {
                //   List<TypeDescription> children = schema.getChildren();
                //   ColumnVector[] fieldVector = new ColumnVector[children.size()];
                //   for(int i=0; i < fieldVector.length; ++i) {
                //     fieldVector[i] = createColumn(children.get(i), version, maxSize);
                //   }
                //   return new UnionColumnVector(maxSize,
                //       fieldVector);
                // }
                // case LIST: {
                //   List<TypeDescription> children = schema.getChildren();
                //   return new ListColumnVector(maxSize,
                //       createColumn(children.get(0), version, maxSize));
                // }
                // case MAP: {
                //   List<TypeDescription> children = schema.getChildren();
                //   return new MapColumnVector(maxSize,
                //       createColumn(children.get(0), version, maxSize),
                //       createColumn(children.get(1), version, maxSize));
                // }
                default:
                    throw new IllegalArgumentException("Unhandled type " + typeDesc.getCategory());
            }
        }
    }
    
    public static abstract class ColumnVectorCopierImpl implements ColumnVectorCopier{
        @Override
        public void deepCopy(ColumnVector srcVec, int srcRow,
            ColumnVector destVec, int destRow,
            int length
        ) {
            System.arraycopy(srcVec.isNull, srcRow, destVec.isNull, destRow, length);
            if (!srcVec.noNulls) {
                destVec.noNulls = true;
            }
            if (!srcVec.isRepeating) {
                destVec.isRepeating = false;
            }
        }
    }
    
    public static class LongColumnVectorCopier extends ColumnVectorCopierImpl 
    {

        @Override
        public void deepCopy(ColumnVector srcVec, int srcRow,
            ColumnVector destVec, int destRow,
            int length
        ) {
            LongColumnVector src = (LongColumnVector)srcVec;
            LongColumnVector dest = (LongColumnVector)destVec;

            super.deepCopy(srcVec, srcRow, destVec, destRow, length);
            System.arraycopy(src.vector, srcRow, dest.vector, destRow, length);
        }
        
    }
    
    public static class DateColumnVectorCopier extends ColumnVectorCopierImpl 
    {

        @Override
        public void deepCopy(ColumnVector srcVec, int srcRow,
            ColumnVector destVec, int destRow,
            int length
        ) {
            DateColumnVector src = (DateColumnVector)srcVec;
            DateColumnVector dest = (DateColumnVector)destVec;

            super.deepCopy(srcVec, srcRow, destVec, destRow, length);
            System.arraycopy(src.vector, srcRow, dest.vector, destRow, length);
        }
        
    }
    
    
    public static class TimestampColumnVectorCopier extends ColumnVectorCopierImpl 
    {
        @Override
        public void deepCopy(ColumnVector srcVec, int srcRow,
            ColumnVector destVec, int destRow,
            int length
        ) {
            TimestampColumnVector src = (TimestampColumnVector)srcVec;
            TimestampColumnVector dest = (TimestampColumnVector)destVec;

            super.deepCopy(srcVec, srcRow, destVec, destRow, length);
            System.arraycopy(src.nanos, srcRow, dest.nanos, destRow, length);
            System.arraycopy(src.time, srcRow, dest.time, destRow, length);
        }
    }
    
    public static class DoubleColumnVectorCopier extends ColumnVectorCopierImpl 
    {
        @Override
        public void deepCopy(ColumnVector srcVec, int srcRow,
            ColumnVector destVec, int destRow,
            int length
        ) {
            DoubleColumnVector src = (DoubleColumnVector)srcVec;
            DoubleColumnVector dest = (DoubleColumnVector)destVec;

            super.deepCopy(srcVec, srcRow, destVec, destRow, length);
            System.arraycopy(src.vector, srcRow, dest.vector, destRow, length);
        }
    }
    
    public static class DecimalColumnVectorCopier extends ColumnVectorCopierImpl 
    {
        @Override
        public void deepCopy(ColumnVector srcVec, int srcRow,
            ColumnVector destVec, int destRow,
            int length
        ) {
            DecimalColumnVector src = (DecimalColumnVector)srcVec;
            DecimalColumnVector dest = (DecimalColumnVector)destVec;
            if (src.precision != dest.precision || src.scale != dest.scale) {
                LOG.error("Two decimal columns have different precision in a deep copy process!");
                System.exit(1);
            }

            super.deepCopy(srcVec, srcRow, destVec, destRow, length);
            while (length > 0) {
                dest.set(destRow, src.vector[srcRow]);
                destRow++;
                srcRow++;
                length--;
            }
            
        }
    }
    
    public static class BytesColumnVectorCopier extends ColumnVectorCopierImpl 
    {
        @Override
        public void deepCopy(ColumnVector srcVec, int srcRow,
            ColumnVector destVec, int destRow,
            int length
        ) {
            BytesColumnVector src = (BytesColumnVector)srcVec;
            BytesColumnVector dest = (BytesColumnVector)destVec;

            super.deepCopy(srcVec, srcRow, destVec, destRow, length);
            // 为了进行深拷贝，需要dest的sharedBuffer不为null
            // 由于不能直接获取sharedBuffer是否为null，因此采用bufferSize()方法间接获取
            if (dest.bufferSize() == 0) {
                dest.initBuffer();
            }
            while (length > 0) {
                if (src.vector[srcRow] == null) {
                    throw new NullPointerException(
                        String.format("No element in row %d of source byte column.", srcRow));
                }
                dest.setVal(destRow, src.vector[srcRow], src.start[srcRow], src.length[srcRow]);
                destRow++;
                srcRow++;
                length--;
            }
        }
    }
    
    /**
     * 创建模式对应的rowBatch每一列的get方法
     * @param schema
     * @return
     */
    public static ElemGetter[] createElemGetters(TypeDescription schema) {
        ElemGetter[] elemGetters = null;
        if (schema.getCategory().isPrimitive()) {
            elemGetters = new ElemGetter[]{ElemGetter.newInstance(schema)};
        }
        else if (schema.getCategory().equals(TypeDescription.Category.STRUCT)) {
            List<TypeDescription> children = schema.getChildren();
            elemGetters = new ElemGetter[children.size()];
            for (int i = 0; i < elemGetters.length; ++i) {
                elemGetters[i] = ElemGetter.newInstance(children.get(i));
            }
        }
        else {
            throw new IllegalArgumentException("Unhandled type " + schema.getCategory());
        }
        
        return elemGetters;
    }
    
    public static interface ElemGetter {
        // 获取Java类型系统下的一个元素
        public Object getElem(ColumnVector colVec, int row);
        // 获取Hadoop类型系统下的一个元素
        public WritableComparable getHadoopElem(ColumnVector colVec, int row);
        
        public static ElemGetter newInstance(TypeDescription typeDesc) {
            switch (typeDesc.getCategory()) {
                case BOOLEAN:
                    return new BooleanElemGetter();
                case BYTE:
                    return new ByteElemGetter();
                case SHORT:
                    return new ShortElemGetter();
                case INT:
                    return new IntElemGetter();
                case LONG:
                  return new LongElemGetter();
                case DATE:
                    return new DateElemGetter();
                case TIMESTAMP:
                case TIMESTAMP_INSTANT:
                  return new TimestampElemGetter();
                case FLOAT:
                case DOUBLE:
                  return new DoubleElemGetter();
                case DECIMAL:
                    return new DecimalElemGetter();
                case STRING:
                case BINARY:
                case CHAR:
                case VARCHAR:
                  return new BytesElemGetter();

                default:
                    throw new IllegalArgumentException("Unhandled type " + typeDesc.getCategory());
            }
        }
    }
    
    public static abstract class ElemGetterImpl implements ElemGetter {}
    
    public static class BooleanElemGetter extends ElemGetterImpl{

        @Override
        public Object getElem(ColumnVector colVec, int row) {
            LongColumnVector vec = (LongColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return vec.vector[0] == 1 ? true : false;
            } else {
                return vec.vector[row] == 1 ? true : false;
            }
        }

        @Override
        public BooleanWritable getHadoopElem(ColumnVector colVec, int row) {
            LongColumnVector vec = (LongColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return new BooleanWritable(vec.vector[0] == 1 ? true : false);
            } else {
                return new BooleanWritable(vec.vector[row] == 1 ? true : false);
            }
        }
        
    }
    
    public static class ByteElemGetter extends ElemGetterImpl{

        @Override
        public Object getElem(ColumnVector colVec, int row) {
            LongColumnVector vec = (LongColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return (byte)(vec.vector[0]);
            } else {
                return (byte)(vec.vector[row]);
            }
        }

        @Override
        public ByteWritable getHadoopElem(ColumnVector colVec, int row) {
            LongColumnVector vec = (LongColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return new ByteWritable((byte)(vec.vector[0]));
            } else {
                return new ByteWritable((byte)(vec.vector[row]));
            }
        }
        
    }
    
    public static class ShortElemGetter extends ElemGetterImpl{

        @Override
        public Object getElem(ColumnVector colVec, int row) {
            LongColumnVector vec = (LongColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return (short)(vec.vector[0]);
            } else {
                return (short)(vec.vector[row]);
            }
        }

        @Override
        public ShortWritable getHadoopElem(ColumnVector colVec, int row) {
            LongColumnVector vec = (LongColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return new ShortWritable((short)(vec.vector[0]));
            } else {
                return new ShortWritable((short)(vec.vector[row]));
            }
        }
        
    }
    
    public static class IntElemGetter extends ElemGetterImpl{

        @Override
        public Object getElem(ColumnVector colVec, int row) {
            LongColumnVector vec = (LongColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return (int)(vec.vector[0]);
            } else {
                return (int)(vec.vector[row]);
            }
        }

        @Override
        public IntWritable getHadoopElem(ColumnVector colVec, int row) {
            LongColumnVector vec = (LongColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return new IntWritable((int)(vec.vector[0]));
            } else {
                return new IntWritable((int)(vec.vector[row]));
            }
        }
        
    }
    
    public static class LongElemGetter extends ElemGetterImpl{

        @Override
        public Object getElem(ColumnVector colVec, int row) {
            LongColumnVector vec = (LongColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return vec.vector[0];
            } else {
                return vec.vector[row];
            }
        }

        @Override
        public LongWritable getHadoopElem(ColumnVector colVec, int row) {
            LongColumnVector vec = (LongColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return new LongWritable(vec.vector[0]);
            } else {
                return new LongWritable(vec.vector[row]);
            }
        }
        
    }

    // 获取java.util.Date类型的DateElemGetter，由于ORC采用daysSinceEpoch存储日期，因此不采用此类型
    // public static class DateElemGetter extends ElemGetterImpl {

    //     @Override
    //     public Object getElem(ColumnVector colVec, int row) {
    //         DateColumnVector vec = (DateColumnVector)colVec;
    //         if (!vec.noNulls && vec.isNull[row]) {
    //             return null;
    //         } else if (vec.isRepeating) {
    //             return new Date(vec.vector[0]);
    //         } else {
    //             return new Date(vec.vector[row]);
    //         }
    //     }

    //     @Override
    //     public Writable getWritableElem(ColumnVector colVec, int row) {
    //         DateColumnVector vec = (DateColumnVector)colVec;
    //         if (!vec.noNulls && vec.isNull[row]) {
    //             return null;
    //         } else if (vec.isRepeating) {
    //             return new DateWritable(new Date(vec.vector[0]));
    //         } else {
    //             return new DateWritable(new Date(vec.vector[row]));
    //         }
    //     }
        
    // }
    
    // 获取java.time.LocalDate类型的DateElemGetter
    public static class DateElemGetter extends ElemGetterImpl {

        @Override
        public Object getElem(ColumnVector colVec, int row) {
            DateColumnVector vec = (DateColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return LocalDate.ofEpochDay(vec.vector[0]);
            } else {
                return LocalDate.ofEpochDay(vec.vector[row]);
            }
        }

        @Override
        public DateWritable getHadoopElem(ColumnVector colVec, int row) {
            DateColumnVector vec = (DateColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return new DateWritable((int)vec.vector[0]);
            } else {
                return new DateWritable((int)vec.vector[row]);
            }
        }
        
    }
    
    public static class TimestampElemGetter extends ElemGetterImpl 
    {
        @Override
        public Object getElem(ColumnVector colVec, int row) {
            TimestampColumnVector vec = (TimestampColumnVector)colVec;
            Timestamp result = null;
            if (!vec.noNulls && vec.isNull[row]) {
                return result;
            } else if (vec.isRepeating) {
                Timestamp scratchTimestamp = vec.asScratchTimestamp(0);
                result = new Timestamp(scratchTimestamp.getTime());
                result.setNanos(scratchTimestamp.getNanos());
                return result;
            } else {
                Timestamp scratchTimestamp = vec.asScratchTimestamp(row);
                result = new Timestamp(scratchTimestamp.getTime());
                result.setNanos(scratchTimestamp.getNanos());
                return result;
            }
        }

        @Override
        public OrcTimestamp getHadoopElem(ColumnVector colVec, int row) {
            TimestampColumnVector vec = (TimestampColumnVector)colVec;
            OrcTimestamp result = null;
            if (!vec.noNulls && vec.isNull[row]) {
                return result;
            } else if (vec.isRepeating) {
                Timestamp scratchTimestamp = vec.asScratchTimestamp(0);
                result = new OrcTimestamp(scratchTimestamp.getTime());
                result.setNanos(scratchTimestamp.getNanos());
                return result;
            } else {
                Timestamp scratchTimestamp = vec.asScratchTimestamp(row);
                result = new OrcTimestamp(scratchTimestamp.getTime());
                result.setNanos(scratchTimestamp.getNanos());
                return result;
            }
        }
    }
    
    public static class DoubleElemGetter extends ElemGetterImpl 
    {
        @Override
        public Object getElem(ColumnVector colVec, int row) {
            DoubleColumnVector vec = (DoubleColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return vec.vector[0];
            } else {
                return vec.vector[row];
            }
        }

        @Override
        public DoubleWritable getHadoopElem(ColumnVector colVec, int row) {
            DoubleColumnVector vec = (DoubleColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return new DoubleWritable(vec.vector[0]);
            } else {
                return new DoubleWritable(vec.vector[row]);
            }
        }
    }
    
    public static class DecimalElemGetter extends ElemGetterImpl 
    {
        @Override
        public HiveDecimalWritable getHadoopElem(ColumnVector colVec, int row) {
            DecimalColumnVector vec = (DecimalColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return vec.vector[0];
            } else {
                return vec.vector[row];
            }
        }

        @Override
        public Object getElem(ColumnVector colVec, int row) {
            DecimalColumnVector vec = (DecimalColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return vec.vector[0].getHiveDecimal().bigDecimalValue();
            } else {
                return vec.vector[row].getHiveDecimal().bigDecimalValue();
            }
        }
    }
    
    public static class BytesElemGetter extends ElemGetterImpl 
    {

        @Override
        public Object getElem(ColumnVector colVec, int row) {
            BytesColumnVector vec = (BytesColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return vec.toString(0);
            } else {
                return vec.toString(row);
            }
        }

        @Override
        public Text getHadoopElem(ColumnVector colVec, int row) {
            BytesColumnVector vec = (BytesColumnVector)colVec;
            if (!vec.noNulls && vec.isNull[row]) {
                return null;
            } else if (vec.isRepeating) {
                return new Text(vec.toString(0));
            } else {
                return new Text(vec.toString(row));
            }
        }
        
    }
    
    /**
     * 创建模式对应的rowBatch每一列的set方法
     * @param schema
     * @return
     */
    public static ElemSetter[] createElemSetters(TypeDescription schema) {
        ElemSetter[] elemSetters = null;
        if (schema.getCategory().isPrimitive()) {
            elemSetters = new ElemSetter[]{ElemSetter.newInstance(schema)};
        }
        else if (schema.getCategory().equals(TypeDescription.Category.STRUCT)) {
            List<TypeDescription> children = schema.getChildren();
            elemSetters = new ElemSetter[children.size()];
            for (int i = 0; i < elemSetters.length; ++i) {
                elemSetters[i] = ElemSetter.newInstance(children.get(i));
            }
        }
        else {
            throw new IllegalArgumentException("Unhandled type " + schema.getCategory());
        }
        
        return elemSetters;
    }
    
    public static interface ElemSetter {
        // 设置Java类型系统下的一个元素
        public void setElem(ColumnVector colVec, int row, Object elem);
        // 设置Hadoop类型系统下的一个元素
        public void setHadoopElem(ColumnVector colVec, int row, WritableComparable elem);
        
        public static ElemSetter newInstance(TypeDescription typeDesc) {
            switch (typeDesc.getCategory()) {
                case BOOLEAN:
                    return new BooleanElemSetter();
                case BYTE:
                    return new ByteElemSetter();
                case SHORT:
                    return new ShortElemSetter();
                case INT:
                    return new IntElemSetter();
                case LONG:
                  return new LongElemSetter();
                case DATE:
                    return new DateElemSetter();
                case TIMESTAMP:
                case TIMESTAMP_INSTANT:
                  return new TimestampElemSetter();
                case FLOAT:
                case DOUBLE:
                  return new DoubleElemSetter();
                case DECIMAL:
                    return new DecimalElemSetter();
                case STRING:
                case BINARY:
                case CHAR:
                case VARCHAR:
                  return new BytesElemSetter();

                default:
                    throw new IllegalArgumentException("Unhandled type " + typeDesc.getCategory());
            }
        }
    }
    
    public static abstract class ElemSetterImpl implements ElemSetter {}
    
    public static class BooleanElemSetter extends ElemSetterImpl{

        @Override
        public void setElem(ColumnVector colVec, int row, Object elem) {
            LongColumnVector col = (LongColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = ((Boolean)elem) == true ? 1L : 0L;
            }
        }

        @Override
        public void setHadoopElem(ColumnVector colVec, int row, WritableComparable elem) {
            LongColumnVector col = (LongColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = ((BooleanWritable)elem).get() == true ? 1L : 0L;
            }
        }
        
    }
    
    public static class ByteElemSetter extends ElemSetterImpl{

        @Override
        public void setElem(ColumnVector colVec, int row, Object elem) {
            LongColumnVector col = (LongColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = ((Byte)elem).longValue();
            }
        }

        @Override
        public void setHadoopElem(ColumnVector colVec, int row, WritableComparable elem) {
            LongColumnVector col = (LongColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = (long)(((ByteWritable)elem).get());
            }
        }
        
    }
    
    public static class ShortElemSetter extends ElemSetterImpl{

        @Override
        public void setElem(ColumnVector colVec, int row, Object elem) {
            LongColumnVector col = (LongColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = ((Short)elem).longValue();
            }
        }

        @Override
        public void setHadoopElem(ColumnVector colVec, int row, WritableComparable elem) {
            LongColumnVector col = (LongColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = (long)(((ShortWritable)elem).get());
            }
        }
        
    }
    
    public static class IntElemSetter extends ElemSetterImpl{

        @Override
        public void setElem(ColumnVector colVec, int row, Object elem) {
            LongColumnVector col = (LongColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = ((Integer)elem).longValue();
            }
        }

        @Override
        public void setHadoopElem(ColumnVector colVec, int row, WritableComparable elem) {
            LongColumnVector col = (LongColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = (long)(((IntWritable)elem).get());
            }
        }
        
    }
    
    public static class LongElemSetter extends ElemSetterImpl{

        @Override
        public void setElem(ColumnVector colVec, int row, Object elem) {
            LongColumnVector col = (LongColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = ((Number)elem).longValue();
            }
        }

        @Override
        public void setHadoopElem(ColumnVector colVec, int row, WritableComparable elem) {
            LongColumnVector col = (LongColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = ((LongWritable)elem).get();
            }
        }
        
    }

    public static class DateElemSetter extends ElemSetterImpl {

        @Override
        public void setElem(ColumnVector colVec, int row, Object elem) {
            DateColumnVector col = (DateColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = ((LocalDate)elem).toEpochDay();
            }
        }

        @Override
        public void setHadoopElem(ColumnVector colVec, int row, WritableComparable elem) {
            DateColumnVector col = (DateColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = ((DateWritable)elem).getDays();
            }
        }
        
    }
    
    public static class TimestampElemSetter extends ElemSetterImpl 
    {
        
        @Override
        public void setElem(ColumnVector colVec, int row, Object elem) {
            TimestampColumnVector col = (TimestampColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.set(row, (Timestamp)elem);    
            }
        }

        @Override
        public void setHadoopElem(ColumnVector colVec, int row, WritableComparable elem) {
            TimestampColumnVector col = (TimestampColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.set(row, ((OrcTimestamp)elem));
            }
        }
    }
    
    public static class DoubleElemSetter extends ElemSetterImpl 
    {
        @Override
        public void setElem(ColumnVector colVec, int row, Object elem) {
            DoubleColumnVector col = (DoubleColumnVector)colVec;
            if (elem == null || elem.equals(Double.NaN)) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = ((Number)elem).doubleValue();
            }
        }

        @Override
        public void setHadoopElem(ColumnVector colVec, int row, WritableComparable elem) {
            DoubleColumnVector col = (DoubleColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.vector[row] = ((DoubleWritable)elem).get();
            }
        }
    }
    
    public static class DecimalElemSetter extends ElemSetterImpl 
    {
        @Override
        public void setElem(ColumnVector colVec, int row, Object elem) {
            DecimalColumnVector col = (DecimalColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.set(row, HiveDecimal.create((BigDecimal)elem));
            }
        }

        @Override
        public void setHadoopElem(ColumnVector colVec, int row, WritableComparable elem) {
            DecimalColumnVector col = (DecimalColumnVector)colVec;
            if (elem == null) {
                col.noNulls = false;
                col.isNull[row] = true;
            }
            else {
                col.set(row, (HiveDecimalWritable)elem);
            }
        }
    }
    
    public static class BytesElemSetter extends ElemSetterImpl 
    {

        @Override
        public void setElem(ColumnVector colVec, int row, Object elem) {
            BytesColumnVector vec = (BytesColumnVector)colVec;
            if (elem == null || elem.equals("")) {
                vec.noNulls = false;
                vec.isNull[row] = true;
            }
            else {
                if (vec.bufferSize() == 0) {
                    vec.initBuffer();
                }
                vec.setVal(row, ((String)elem).getBytes());    
            }
        }

        @Override
        public void setHadoopElem(ColumnVector colVec, int row, WritableComparable elem) {
            BytesColumnVector vec = (BytesColumnVector)colVec;
            if (elem == null || elem.equals("")) {
                vec.noNulls = false;
                vec.isNull[row] = true;
            }
            else {
                if (vec.bufferSize() == 0) {
                    vec.initBuffer();
                }
                vec.setVal(row, ((Text)elem).getBytes());
            }
        }
        
    }

}
