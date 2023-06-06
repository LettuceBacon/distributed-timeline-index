package xyz.mfj.enhanced;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import xyz.mfj.enhanced.ColumnVectorEnhance.BytesColumnVectorCopier;
import xyz.mfj.enhanced.ColumnVectorEnhance.BytesElemGetter;
import xyz.mfj.enhanced.ColumnVectorEnhance.BytesElemSetter;
import xyz.mfj.enhanced.ColumnVectorEnhance.ColumnVectorCopier;
import xyz.mfj.enhanced.ColumnVectorEnhance.DateElemGetter;
import xyz.mfj.enhanced.ColumnVectorEnhance.DateElemSetter;
import xyz.mfj.enhanced.ColumnVectorEnhance.DecimalElemGetter;
import xyz.mfj.enhanced.ColumnVectorEnhance.ElemGetter;
import xyz.mfj.enhanced.ColumnVectorEnhance.ElemSetter;
import xyz.mfj.enhanced.ColumnVectorEnhance.LongColumnVectorCopier;
import xyz.mfj.enhanced.ColumnVectorEnhance.LongElemGetter;
import xyz.mfj.enhanced.ColumnVectorEnhance.LongElemSetter;

public class ColumnVectorEnhanceTest {
    @Test
    public void intTypeCopierTest() {
        TypeDescription intType = TypeDescription.fromString("int");
        LongColumnVector srcVec = new LongColumnVector(5);
        LongColumnVector destVec = new LongColumnVector(10);
        for (int i = 0; i < 5; i++) {
            srcVec.vector[i] = i;
        }
        
        ColumnVectorCopier[] copiers = ColumnVectorEnhance.createCopiers(intType);
        assertEquals(copiers.length, 1);
        ColumnVectorCopier intCopier = copiers[0];
        assertNotNull(intCopier);
        intCopier.deepCopy(srcVec, 0, destVec, 0, 5);
        assertTrue(Arrays.equals(destVec.vector, new long[]{0L, 1L, 2L, 3L, 4L, 0L, 0L, 0L, 0L, 0L}));
        intCopier.deepCopy(destVec, 5, srcVec, 0, 5);
        assertTrue(Arrays.equals(srcVec.vector, new long[]{0L, 0L, 0L, 0L, 0L}));
    }
    
    @Test
    public void dateTypeCopierTest() {
        TypeDescription dateType = TypeDescription.fromString("date");
        DateColumnVector srcVec = new DateColumnVector(5);
        DateColumnVector destVec = new DateColumnVector(10);
        for (int i = 0; i < 5; i++) {
            srcVec.vector[i] = i;
        }
        
        ColumnVectorCopier[] copiers = ColumnVectorEnhance.createCopiers(dateType);
        assertEquals(copiers.length, 1);
        ColumnVectorCopier dateCopier = copiers[0];
        assertNotNull(dateCopier);
        dateCopier.deepCopy(srcVec, 0, destVec, 0, 5);
        assertTrue(Arrays.equals(destVec.vector, new long[]{0L, 1L, 2L, 3L, 4L, 0L, 0L, 0L, 0L, 0L}));
        dateCopier.deepCopy(destVec, 5, srcVec, 0, 5);
        assertTrue(Arrays.equals(srcVec.vector, new long[]{0L, 0L, 0L, 0L, 0L}));
    }
    
    @Test
    public void decimalTypeCopierTest() {
        TypeDescription decimalType = TypeDescription.fromString("decimal(10,2)");
        short scale = 2;
        short precision = 10;
        DecimalColumnVector srcVec = new DecimalColumnVector(5, precision, scale);
        DecimalColumnVector destVec = new DecimalColumnVector(10, precision, scale);
        for (int i = 0; i < 5; i++) {
            srcVec.vector[i] = new HiveDecimalWritable("0.0" + i);
        }
        
        ColumnVectorCopier[] copiers = ColumnVectorEnhance.createCopiers(decimalType);
        assertEquals(copiers.length, 1);
        ColumnVectorCopier decimalCopier = copiers[0];
        assertNotNull(decimalCopier);
        decimalCopier.deepCopy(srcVec, 0, destVec, 0, 5);
        for (int i = 0; i < 5; i++) {
            assertEquals(destVec.vector[i], new HiveDecimalWritable("0.0" + i));
        }
        for (int i = 5; i < 10; i++) {
            assertEquals(destVec.vector[i], new HiveDecimalWritable("0"));
        }
        decimalCopier.deepCopy(destVec, 5, srcVec, 0, 5);
        for (int i = 0; i < 5; i++) {
            assertEquals(srcVec.vector[i], new HiveDecimalWritable("0"));
        }
    }
    
    @Test
    public void stringTypeCopierTest() {
        TypeDescription stringType = TypeDescription.fromString("string");
        int bigSize = 10;
        int smallSize = 5;
        BytesColumnVector srcVec = null;
        BytesColumnVector destVec = null;
        String[] teststr = new String[]{"test0", "test1", "test2", "test3", "test4"
            , "test5", "test6", "test7", "test8", "test9"};
        ColumnVectorCopier[] copiers = ColumnVectorEnhance.createCopiers(stringType);
        assertEquals(copiers.length, 1);
        ColumnVectorCopier stringCopier = copiers[0];
        assertNotNull(stringCopier);
        
        // 前提：整个src拷贝到dest任意位置
        // dest有没有初始化sharedBuffer？初始化：未初始化
        // src需拷贝的部分是否有空值？有空值：无空值
        // src能否全部拷贝到dest中？全拷贝：不能全拷贝

        
        // dest未初始化，src无空值，src不能全拷贝
        destVec = new BytesColumnVector(smallSize);
        srcVec = new BytesColumnVector(bigSize);
        for (int i = 0; i < srcVec.vector.length; i++) {
            byte[] buffer = teststr[i].getBytes();
            srcVec.setRef(i, buffer, 0, buffer.length);
        }
        try {
            stringCopier.deepCopy(srcVec, 0, destVec, 0, srcVec.vector.length);
        } catch (Exception e) {
            assertEquals(e.getClass(), ArrayIndexOutOfBoundsException.class);
        }
        
        // dest初始化，src有空值，src可以全拷贝
        destVec = new BytesColumnVector(bigSize);
        destVec.initBuffer();
        srcVec = new BytesColumnVector(smallSize);
        for (int i = 0; i < 3; i++) {
            byte[] buffer = teststr[i].getBytes();
            srcVec.setRef(i, buffer, 0, buffer.length);
        }
        try {
            stringCopier.deepCopy(srcVec, 0, destVec, 0, srcVec.vector.length);
            
        } catch (Exception e) {
            assertEquals(e.getClass(), NullPointerException.class);
        }
        
        // dest初始化，src有空值，src不能全拷贝
        destVec = new BytesColumnVector(smallSize);
        destVec.initBuffer();
        srcVec = new BytesColumnVector(bigSize);
        for (int i = 0; i < 3; i++) {
            byte[] buffer = teststr[i].getBytes();
            srcVec.setRef(i, buffer, 0, buffer.length);
        }
        try {
            stringCopier.deepCopy(srcVec, 0, destVec, 0, srcVec.vector.length);
        } catch (Exception e) {
            assertEquals(e.getClass(), ArrayIndexOutOfBoundsException.class);
        }
        
        // dest未初始化，src无空值，src可以全拷贝
        destVec = new BytesColumnVector(bigSize);
        srcVec = new BytesColumnVector(smallSize);
        for (int i = 0; i < srcVec.vector.length; i++) {
            byte[] buffer = teststr[i].getBytes();
            srcVec.setRef(i, buffer, 0, buffer.length);
        }
        stringCopier.deepCopy(srcVec, 0, destVec, 0, srcVec.vector.length);
        for (int i = 0; i < srcVec.vector.length; i++) {
            assertEquals(destVec.toString(i), teststr[i]);
        }
    }
    
    // TypeDescription timestampType = TypeDescription.fromString("timestamp");
    // TypeDescription doubleType = TypeDescription.fromString("double");
    
    @Test
    public void rowBatchCopierTest() {
        TypeDescription structType = TypeDescription.fromString("struct<x:bigint,y:char(5)>");
        
        ColumnVectorCopier[] rowBatchCopier = ColumnVectorEnhance.createCopiers(structType);
        assertEquals(rowBatchCopier.length, 2);
        assertEquals(rowBatchCopier[0].getClass(), LongColumnVectorCopier.class);
        assertEquals(rowBatchCopier[1].getClass(), BytesColumnVectorCopier.class);
    }
    
    @Test
    public void LongColumnVectorGetterSetterest() {
        LongColumnVector col = new LongColumnVector(5);
        ElemSetter setter = ElemSetter.newInstance(TypeDescription.createLong());
        for (int i = 0; i < 2; i++) {
            setter.setElem(col, i, Long.valueOf(i));
        }
        setter.setElem(col, 2, null);
        for (int i = 3; i < 5; i++) {
            setter.setHadoopElem(col, i, new LongWritable(i));
        }
        
        ElemGetter getter = ElemGetter.newInstance(TypeDescription.createLong());
        assertEquals(getter.getClass(), LongElemGetter.class);
        assertEquals(getter.getElem(col, 0), 0L);
        assertEquals(getter.getElem(col, 1), 1L);
        assertEquals(getter.getElem(col, 2), null);
        assertEquals(getter.getHadoopElem(col, 3), new LongWritable(3L));
    }
    
    @Test
    public void DecimalColumnVectorGetterTest() {
        DecimalColumnVector col = new DecimalColumnVector(5, 10, 2);
        ElemSetter setter = ElemSetter.newInstance(
            TypeDescription.createDecimal()
                .withPrecision(10)
                .withScale(2)
        );
        for (int i = 0; i < 2; i++) {
            setter.setElem(col, i, BigDecimal.valueOf(i));
        }
        for (int i = 2; i < 5; i++) {
            setter.setHadoopElem(col, i, new HiveDecimalWritable(String.valueOf(i)));
        }
        
        ElemGetter getter = ElemGetter.newInstance(TypeDescription.createDecimal());
        assertEquals(getter.getClass(), DecimalElemGetter.class);
        assertEquals(getter.getElem(col, 0), new BigDecimal("0"));
        assertEquals(getter.getElem(col, 1), new BigDecimal("1"));
        assertEquals(getter.getHadoopElem(col, 3), new HiveDecimalWritable(3L));
    }
    
    @Test
    public void BytesColumnVectorGetterTest() {
        BytesColumnVector col = new BytesColumnVector(5);
        String[] teststr = new String[]{"test0", "test1", null, "test3", "test4"
            , "test5", "test6", "test7", "test8", "test9"};
        ElemSetter setter = ElemSetter.newInstance(TypeDescription.createString());
        for (int i = 0; i < 2; i++) {
            setter.setElem(col, i, teststr[i]);
        }
        for (int i = 2; i < 5; i++) {
            setter.setHadoopElem(col, i, teststr[i] == null ? null : new Text(teststr[i]));
        }
        
        ElemGetter getter = ElemGetter.newInstance(TypeDescription.createString());
        assertEquals(getter.getClass(), BytesElemGetter.class);
        assertEquals(getter.getElem(col, 0), teststr[0]);
        assertEquals(getter.getElem(col, 1), teststr[1]);
        assertEquals(getter.getElem(col, 2), teststr[2]);
        assertEquals(getter.getHadoopElem(col, 3), new Text(teststr[3]));
    }
    
    @Test
    public void DateColumnVectorGetterTest() {
        DateColumnVector col = new DateColumnVector(5);
        ElemSetter setter = ElemSetter.newInstance(TypeDescription.createDate());
        for (int i = 0; i < 2; i++) {
            setter.setElem(col, i, LocalDate.ofEpochDay(i));
        }
        for (int i = 2; i < 5; i++) {
            setter.setHadoopElem(col, i, new DateWritable(i));
        }
        
        ElemGetter getter = ElemGetter.newInstance(TypeDescription.createDate());
        assertEquals(getter.getClass(), DateElemGetter.class);
        assertEquals(getter.getElem(col, 0), LocalDate.ofEpochDay(0));
        assertEquals(getter.getElem(col, 1), LocalDate.ofEpochDay(1));
        assertEquals(getter.getHadoopElem(col, 3), new DateWritable(3));
    }
    
    
    @Test
    public void rowBatchElemGetterSetterTest() {
        TypeDescription structType = TypeDescription.fromString("struct<x:bigint,y:char(5)>");
        ElemGetter[] getters = ColumnVectorEnhance.createElemGetters(structType);
        assertEquals(getters.length, 2);
        assertEquals(getters[0].getClass(), LongElemGetter.class);
        assertEquals(getters[1].getClass(), BytesElemGetter.class);
        
        TypeDescription primitiveType = TypeDescription.fromString("date");
        getters = ColumnVectorEnhance.createElemGetters(primitiveType);
        assertEquals(getters.length, 1);
        assertEquals(getters[0].getClass(), DateElemGetter.class);
        
        ElemSetter[] setters = ColumnVectorEnhance.createElemSetters(structType);
        assertEquals(setters.length, 2);
        assertEquals(setters[0].getClass(), LongElemSetter.class);
        assertEquals(setters[1].getClass(), BytesElemSetter.class);
        
        setters = ColumnVectorEnhance.createElemSetters(primitiveType);
        assertEquals(setters.length, 1);
        assertEquals(setters[0].getClass(), DateElemSetter.class);
    }
    
}
