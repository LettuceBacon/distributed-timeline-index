package xyz.mfj.utils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.apache.orc.mapred.OrcTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypeUtil {
    private static Logger log = LoggerFactory.getLogger(TypeUtil.class);
    
    public static Object zeroOf(TypeDescription type) {
        switch (type.getCategory()) {
            case BYTE:
                return (byte)0;
            case SHORT:
                return (short)0;
            case INT:
                return 0;
            case LONG:
              return 0L;
            case DATE:
                return LocalDate.ofEpochDay(0L);
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return new Timestamp(0L);
            case FLOAT:
                return (float)0.0;
            case DOUBLE:
                return 0.0;
            case DECIMAL:
                return BigDecimal.ZERO;

            default:
                throw new IllegalArgumentException("Type {} doesn't have zero value." + type.getCategory());
        }
        
    }
    
    // 根据模式获取hadoop类型系统的类型的零值
    public static WritableComparable hadoopZeroOf(TypeDescription type) {
        switch (type.getCategory()) {
            case BYTE:
                return new ByteWritable((byte)0);
            case SHORT:
                return new ShortWritable((short)0);
            case INT:
                return new IntWritable(0);
            case LONG:
              return new LongWritable(0L);
            case DATE:
                return new DateWritable(0);
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return new OrcTimestamp(0L);
            case FLOAT:
                return new FloatWritable(0);
            case DOUBLE:
                return new DoubleWritable(0);
            case DECIMAL:
                return new HiveDecimalWritable(0L);

            default:
                throw new IllegalArgumentException("Type {} doesn't have zero value." + type.getCategory());
        }
        
    }
    
    public static Object minOf(TypeDescription type) {
        switch (type.getCategory()) {
            case BYTE:
                return Byte.MIN_VALUE;
            case SHORT:
                return Short.MIN_VALUE;
            case INT:
                return Integer.MIN_VALUE;
            case LONG:
                return Long.MIN_VALUE;
            case DATE:
                return LocalDate.MIN;
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return new Timestamp(0);
            case FLOAT:
                return -Float.MAX_VALUE;
            case DOUBLE:
                return -Double.MAX_VALUE;
            case DECIMAL:
                return BigDecimal.valueOf(-Double.MAX_VALUE);

            default:
                throw new IllegalArgumentException("Type {} doesn't have max value!" + type.getCategory());
        }
    }
    
    // 根据模式获取hadoop类型系统的类型的最小值
    public static WritableComparable hadoopMinOf(TypeDescription type) {
        switch (type.getCategory()) {
            case BYTE:
                return new ByteWritable(Byte.MIN_VALUE);
            case SHORT:
                return new ShortWritable(Short.MIN_VALUE);
            case INT:
                return new IntWritable(Integer.MIN_VALUE);
            case LONG:
                return new LongWritable(Long.MIN_VALUE);
            case DATE:
                return new DateWritable((int)LocalDate.MIN.toEpochDay());
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return new OrcTimestamp(0L);
            case FLOAT:
                return new FloatWritable(-Float.MAX_VALUE);
            case DOUBLE:
                return new DoubleWritable(-Double.MAX_VALUE);
            case DECIMAL:
                return new HiveDecimalWritable(HiveDecimal.create(-Double.MAX_VALUE));

            default:
                throw new IllegalArgumentException("Type {} doesn't have max value!" + type.getCategory());
        }
    }
    
    // 根据模式获取hadoop类型系统的类型的最大值
    public static WritableComparable hadoopMaxOf(TypeDescription type) {
        switch (type.getCategory()) {
            case BYTE:
                return new ByteWritable(Byte.MAX_VALUE);
            case SHORT:
                return new ShortWritable(Short.MAX_VALUE);
            case INT:
                return new IntWritable(Integer.MAX_VALUE);
            case LONG:
                return new LongWritable(Long.MAX_VALUE);
            case DATE:
                return new DateWritable((int)LocalDate.MAX.toEpochDay());
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return new OrcTimestamp(253402271999999L);
            case FLOAT:
                return new FloatWritable(Float.MAX_VALUE);
            case DOUBLE:
                return new DoubleWritable(Double.MAX_VALUE);
            case DECIMAL:
                return new HiveDecimalWritable(HiveDecimal.create(Double.MAX_VALUE));

            default:
                throw new IllegalArgumentException("Type {} doesn't have max value!" + type.getCategory());
        }
    }
    
    public static Object maxOf(TypeDescription type) {
        switch (type.getCategory()) {
            case BYTE:
                return Byte.MAX_VALUE;
            case SHORT:
                return Short.MAX_VALUE;
            case INT:
                return Integer.MAX_VALUE;
            case LONG:
                return Long.MAX_VALUE;
            case DATE:
                return LocalDate.MAX;
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return new Timestamp(253402271999999L);
            case FLOAT:
                return Float.MAX_VALUE;
            case DOUBLE:
                return Double.MAX_VALUE;
            case DECIMAL:
                return BigDecimal.valueOf(Double.MAX_VALUE);

            default:
                throw new IllegalArgumentException("Type {} doesn't have max value!" + type.getCategory());
        }
    }
    
    public static boolean isNumeric(TypeDescription type) {
        Category typeCategory = type.getCategory();
        if (typeCategory.equals(Category.BYTE)
            || typeCategory.equals(Category.SHORT)
            || typeCategory.equals(Category.INT)
            || typeCategory.equals(Category.LONG)
            || typeCategory.equals(Category.FLOAT)
            || typeCategory.equals(Category.DOUBLE)
            || typeCategory.equals(Category.DECIMAL)
        ) {
            return true;
        }
        else { 
            return false;
        }
    }
    
    public static PredicateLeaf.Type typeDesc2PredLeafType(TypeDescription type) {
        switch (type.getCategory()) {
            case BOOLEAN:
                return PredicateLeaf.Type.BOOLEAN;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return PredicateLeaf.Type.LONG;
            case DATE:
                return PredicateLeaf.Type.DATE;
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return PredicateLeaf.Type.TIMESTAMP;
            case FLOAT:
            case DOUBLE:
                return PredicateLeaf.Type.FLOAT;
            case DECIMAL:
                return PredicateLeaf.Type.DECIMAL;
            case CHAR:
            case VARCHAR:
            case STRING:
                return PredicateLeaf.Type.STRING;
            
            // case BINARY: 
                
            default:
                throw new IllegalArgumentException("Type {} can't compare by sarg!" + type.getCategory());
        }
    }
    
    public static boolean isDateTime(Object obj) {
        if (obj.getClass().equals(Timestamp.class)
            || obj.getClass().equals(LocalDate.class)
        ) {
            return true;
        }
        else {
            return false;
        }
    }
    
    public static WritableComparable jObj2hadoopObj(Object jObject) {
        if (jObject == null) return null;
        String className = jObject.getClass().getName();
        switch (className) {
            case "byte":
            case "java.lang.Byte":
                return new ByteWritable((byte)jObject);
                
            case "short":
            case "java.lang.Short":
                return new ShortWritable((short)jObject);
            
            case "int":
            case "java.lang.Integer":
                return new IntWritable((int)jObject);

            case "long":
            case "java.lang.Long":
                return new LongWritable((long)jObject);
                
            case "float":
            case "java.lang.Float":
                return new FloatWritable((float)jObject);
                
            case "double":
            case "java.lang.Double":
                return new DoubleWritable((double)jObject);
                
            case "java.math.BigDecimal":
                return new HiveDecimalWritable(((BigDecimal)jObject).toString());
            
            case "java.time.LocalDate":
                return new DateWritable((int)((LocalDate)jObject).toEpochDay());
                
            case "java.sql.Timestamp":
                Timestamp ts = (Timestamp)jObject;
                OrcTimestamp hadoopTs = new OrcTimestamp(ts.getTime());
                hadoopTs.setNanos(ts.getNanos());
                return hadoopTs;
                
            case "java.lang.String":
                return new BytesWritable(((String)jObject).getBytes());
            
        
            default:
                return null;
        }
    }
    
    public static Object hadoopObj2JObj(WritableComparable hadoopObject) {
        if (hadoopObject == null) return null;
        String className = hadoopObject.getClass().getName();
        switch (className) {
            case "org.apache.hadoop.io.ByteWritable":
                return ((ByteWritable)hadoopObject).get();
                
            case "org.apache.hadoop.io.ShortWritable":
                return ((ShortWritable)hadoopObject).get();
            
            case "org.apache.hadoop.io.IntWritable":
                return ((IntWritable)hadoopObject).get();
                
            case "org.apache.hadoop.io.LongWritable":
                return ((LongWritable)hadoopObject).get();
                
            case "org.apache.hadoop.io.FloatWritable":
                return ((FloatWritable)hadoopObject).get();
                
            case "org.apache.hadoop.io.DoubleWritable":
                return ((DoubleWritable)hadoopObject).get();
                
            case "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable":
                return new BigDecimal(((HiveDecimalWritable)hadoopObject).toString());
            
            case "org.apache.hadoop.hive.serde2.io.DateWritable":
                return LocalDate.ofEpochDay(((DateWritable)hadoopObject).getDays());
                
            case "org.apache.orc.mapred.OrcTimestamp":
                OrcTimestamp hadoopTs = (OrcTimestamp)hadoopObject;
                Timestamp ts = new Timestamp(hadoopTs.getTime());
                ts.setNanos(hadoopTs.getNanos());
                return ts;
                
            case "org.apache.hadoop.io.BytesWritable":
                return new String(((BytesWritable)hadoopObject).getBytes());
            
        
            default:
                return null;
        }
    }
    
    public static Object nullOf(TypeDescription type) {
        switch (type.getCategory()) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
            case DECIMAL:
            case STRING:
                return null;
            case FLOAT:
                return Float.NaN;
            case DOUBLE:
                return Double.NaN;

            default:
                throw new IllegalArgumentException("Type {} doesn't have NaN value." + type.getCategory());
        }
    }
}
