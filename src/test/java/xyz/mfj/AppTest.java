package xyz.mfj;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.function.Function;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.hive.serde.serdeConstants;
// import org.apache.hadoop.hive.serde2.OpenCSVSerde;
// import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.SysInfo;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.TypeUtils;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcTimestamp;
import org.junit.jupiter.api.Test;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.reflectasm.PublicConstructorAccess;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.ICSVParser;

// import net.sourceforge.sizeof.SizeOf;
import xyz.mfj.dataDefiniation.ApplicationPeriod;
import xyz.mfj.dataDefiniation.InfixCountingSort;
import xyz.mfj.dataDefiniation.TimelineIndex.VRF;
import xyz.mfj.utils.SerDeUtil;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    @Test
    public void NullDebug() {
        Object abc = null;
        System.out.println(abc == null || abc.equals(null));
    }
    
    @Test
    public void printTimePoint() {
        LocalDate lineitembihStart = LocalDate.parse("1992-01-01");
        LocalDate lineitembihEnd = LocalDate.parse("1998-12-31");
        
        Timestamp YTTRStart = Timestamp.valueOf("2022-01-01 00:00:00.0");
        Timestamp YTTREnd = Timestamp.valueOf("2022-12-31 23:59:59.0");
        
        // 千分之一
        LocalDate dateOneThousandth = LocalDate.ofEpochDay(
            (long)(0.001 * (lineitembihEnd.toEpochDay() - lineitembihStart.toEpochDay()))
        ); // 三天
        Timestamp timestampOneThousandth = new Timestamp(
            (long)(0.001 * (YTTREnd.getTime() - YTTRStart.getTime()))
        ); // 16:45:36
        System.out.println(dateOneThousandth);
        System.out.println(timestampOneThousandth);
        
        // 百分之一
        LocalDate dateOneHundredth = LocalDate.ofEpochDay(
            (long)(0.01 * (lineitembihEnd.toEpochDay() - lineitembihStart.toEpochDay()))
        ); // 二十六天
        Timestamp timestampOneHundredth = new Timestamp(
            (long)(0.01 * (YTTREnd.getTime() - YTTRStart.getTime()))
        ); // 三天加23:36:00
        System.out.println(dateOneHundredth);
        System.out.println(timestampOneHundredth);
    }
    
    @Test
    public void eightBitDebug() {
        int baseMask = (1 << 8) - 1;
        
        System.out.println(255 & baseMask);
        System.out.println(256 & baseMask);
        System.out.println(257 & baseMask);
    }
    
    @Test
    public void tooManyLargeArrays() {
        int counterArraySize = 1 << 8;
        System.out.println(counterArraySize);
        // int[] maxSizeArray = new int[Integer.MAX_VALUE];
        int[] maxSizeArray = new int[671555626];
        // System.out.println(SizeOf.deepSizeOf(maxSizeArray));
    }
    
    @Test
    public void NullValueDebug() {
        System.out.println(Double.NaN);
        System.out.println(Double.NaN + 1.0);
        System.out.println(Double.NaN < 1.0);
        
        TreeMap<Double, Integer> abc = new TreeMap<>();
        abc.put(Double.NaN, null); // 还是应该避免这个计算
        abc.put(1.0, null);
        abc.put(1.1, null);
        System.out.println(abc);
    }
    
    @Test
    public void integerRangeTimestamp() {
        Timestamp minIntTs = new Timestamp(0L);
        Timestamp maxIntTs = new Timestamp((long)Integer.MAX_VALUE);
        System.out.println(minIntTs); // 1970-01-01 08:00:00.0
        System.out.println(maxIntTs); // 1970-01-26 04:31:23.647
        
        minIntTs = Timestamp.valueOf("1970-01-01 08:00:00.0");
        maxIntTs = Timestamp.valueOf("1971-01-01 08:00:00.0");
        System.out.println(maxIntTs.getTime() - minIntTs.getTime());
        // 不到一个月
    }

    @Test
    public void TreeMapConstructComparation() {
        
        long sTime = 0;
        long eTime = 0;
        Random rand = new Random();
        TreeMap<Long, Integer> abc = new TreeMap<>();
        for (int i = 0; i < 1000; i++) {
            abc.put(rand.nextLong(), null);
        }
        
        // init
        // sTime = System.currentTimeMillis();
        sTime = System.nanoTime();
        TreeMap<Long, Integer> wasd = new TreeMap<>(abc);
        // eTime = System.currentTimeMillis();
        eTime = System.nanoTime();
        System.out.println(eTime - sTime); // 1553216 ns
        
        // put
        // sTime = System.currentTimeMillis();
        sTime = System.nanoTime();
        wasd = new TreeMap<>();
        for (Long l : abc.navigableKeySet()) {
            wasd.put(l, null);
        }
        // eTime = System.currentTimeMillis();
        eTime = System.nanoTime();
        System.out.println(eTime - sTime); // 1569307 ns
    }
    
    @Test
    public void nanoTimeTest() {
        long sTime = System.nanoTime();
        long eTime = System.nanoTime();
        System.out.println(eTime - sTime);
    }
    
    public static void insertionSortImperative(long[] input) {
        for (int i = 1; i < input.length; i++) { 
            long key = input[i]; 
            int j = i - 1;
            while (j >= 0 && input[j] > key) {
                input[j + 1] = input[j];
                j = j - 1;
            }
            input[j + 1] = key; 
        }
    }
    
    private static void insertionSort(Comparable[] sortedArray, int size, Comparable elem) {
        int i = size - 1;
        while (i >= 0 && sortedArray[i].compareTo(elem) > 0) {
            sortedArray[i + 1] = sortedArray[i];
            --i;
        }
        sortedArray[i + 1] = elem;
    }
    
    @Test
    public void insertionBestPerformance() {
        // 查看插排在不同数据量下的执行时间
        // 找到初始时执行时间与数据量的关系，再找到那个不再满足关系的数据量，0到该数据量即为插排表现的最佳值区间
        Random rand = new Random();
        int num0 = 10;
        int numn = 500;
        long sNanoTime;
        long eNanoTime;
        // long sTime;
        // long eTime;
        long[] takenNanoTimes = new long[numn - num0];
        long[] takenTimes = new long[numn - num0];
        for (int num = num0; num < numn; num++) {
            // long[] longArray = new long[num];
            LocalDate[] dateArray = new LocalDate[num];
            sNanoTime = System.nanoTime();
            // sTime = System.currentTimeMillis();
            for (int j = 0; j < num; j++) {
                // longArray[j] = (long)(rand.nextInt() >> 4);
                insertionSort(dateArray, j, LocalDate.ofEpochDay((long)(rand.nextInt() >> 4)));
            }
            // insertionSortImperative(longArray);
            for (int j = 0; j < num; j++) {
                // longArray[j]++;
                dateArray[j].toEpochDay();
            }
            eNanoTime = System.nanoTime();
            // eTime = System.currentTimeMillis();
            takenNanoTimes[num - num0] = eNanoTime - sNanoTime;
            // takenTimes[num - num0] = eTime - sTime;
        }
        StringBuilder sb = new StringBuilder()
            .append("Taken times:\n");
        for (int i = 0; i < numn - num0; ++i) {
            sb.append(i + num0)
                .append(':')
                // .append(takenTimes[i])
                // .append("ms, ")
                .append(takenNanoTimes[i])
                .append("ns, ratio:")
                .append(takenNanoTimes[i]/((i + num0)*(i+ num0)))
                .append('\n');
        }
        System.out.println(sb.toString());
    }
    
    // @Test
    // public void smallDataSetSort() {
    //     // 看哪个排序方法更适合小数据量，适合多少的，同时辅助结构不能比其他排序大过一个数量级
    //     // 取最小的那个时间，看它是哪个排序的哪个数据量，
        
    //     // 生成在int >> 4范围内的正的long值
    //     Random rand = new Random();
    //     int num0 = 10;
    //     int numn = 100;
    //     long sTime;
    //     long eTime;
        
    //     long minTime = Long.MAX_VALUE;
    //     int minTimeNum = 0;
    //     long minTimeSize = 0;
        
    //     // 红黑树
    //     for (int i = num0; i < numn; ++i) {
    //         TreeMap<Long, Integer> rbTree = new TreeMap<>();
    //         sTime = System.nanoTime();
    //         for (int j = 0; j < i; j++) {
    //             rbTree.put((long)(rand.nextInt() >> 4), null);
    //         }
    //         for (Long l : rbTree.navigableKeySet()) {
    //             l++;
    //         }
    //         eTime = System.nanoTime();
    //         if (eTime - sTime < minTime) {
    //             minTime = eTime - sTime;
    //             minTimeNum = i;
    //             minTimeSize = SizeOf.deepSizeOf(rbTree);
    //         }
    //     }
    //     System.out.printf("rbTree min-taken time is %d ns, and sort %d numbers by %d bytes struct\n", 
    //         minTime, minTimeNum, minTimeSize);
            
    //     minTime = Long.MAX_VALUE;
    //     minTimeNum = 0;
    //     minTimeSize = 0;
        
    //     // 插入排序
    //     for (int i = num0; i < numn; ++i) {
    //         long[] longArray = new long[i];
    //         sTime = System.nanoTime();
    //         for (int j = 0; j < i; j++) {
    //             longArray[j] = (long)(rand.nextInt() >> 4);
    //         }
    //         insertionSortImperative(longArray);
    //         for (int j = 0; j < i; j++) {
    //             longArray[j]++;
    //         }
    //         eTime = System.nanoTime();
    //         if (eTime - sTime < minTime) {
    //             minTime = eTime - sTime;
    //             minTimeNum = i;
    //             minTimeSize = SizeOf.deepSizeOf(longArray);
    //         }
    //     }
    //     System.out.printf("insertionSorting min-taken time is %d ns, and sort %d numbers by %d bytes struct\n", 
    //         minTime, minTimeNum, minTimeSize);
            
    //     minTime = Long.MAX_VALUE;
    //     minTimeNum = 0;
    //     minTimeSize = 0;
    // }
    
    
    @Test
    public void simulateLongestCommonPrefix() {
        long abc = 18L;
        long wasd = 2L;
        int lcpBitNum = Long.SIZE;
        
        TreeMap<String, Integer> tre = new TreeMap<>();
        tre.put("ok", null);
        System.out.println(tre.get("ok"));
        
        long[] lcpArray = new long[lcpBitNum];
        // 前缀i+1个bit为1的数字的数组
        long[] prefixArray = new long[lcpBitNum];
        for (int i = 0; i < lcpBitNum; ++i) {
            prefixArray[i] = Long.MAX_VALUE >> (Long.SIZE - i - 1) << (Long.SIZE - i - 1);
            lcpArray[i] = prefixArray[i] & abc;
        }
        
        for (int i = 0; i < lcpBitNum; ++i) {
            if (lcpArray[i] != (wasd & prefixArray[i])) {
                lcpBitNum = i;
                break;
            }
        }
    }
    
    @Test
    public void bitMoveCompareToAnd() {
        long sTime;
        long eTime;
        Random rand = new Random();
        long foo;
        
        // bit move
        sTime = System.currentTimeMillis();
        for (int i = 0; i < 100000000; ++i) {
            foo = rand.nextLong() >> 1 << 1;
        }
        eTime= System.currentTimeMillis();
        System.out.printf("bit move %d sec(s)\n", eTime - sTime); // 12s
        
        // and opr
        sTime = System.currentTimeMillis();
        for (int i = 0; i < 100000000; ++i) {
            foo = (Long.MAX_VALUE - 1) & rand.nextLong();
        }
        eTime= System.currentTimeMillis();
        System.out.printf("and opr %d sec(s)\n", eTime - sTime); // 10s
    }
    
    @Test
    public void bitOperation() {
        
        long pone = 1L;
        long ptwo = 2L;
        System.out.println(pone | ptwo);
        long peight = 8L;
        long pten = 10L;
        System.out.println(peight & pten);
        System.out.println(pone & Long.MAX_VALUE);
        
        System.out.println(Long.BYTES);
        System.out.println(Long.SIZE);
        
        System.out.println(pone >> 1 << 1);
        System.out.println(ptwo >> 1 << 1);
        
        System.out.println((Long.MAX_VALUE - 1L) >> 1 << 1);
    }
    
    @Test
    public void overloadFunctionDebug() {
        WritableComparable abc = new OrcTimestamp(2L);
        Function<DateWritable, Integer> date2Int = new Function<DateWritable,Integer>() {

            @Override
            public Integer apply(DateWritable arg0) {
                System.out.println("date");
                return arg0.getDays() / 2;
            }
            
        };
        Function<OrcTimestamp, Integer> timestamp2Int = new Function<OrcTimestamp,Integer>() {

            @Override
            public Integer apply(OrcTimestamp arg0) {
                System.out.println("timestamp");
                return (int)(arg0.getTime() / 2L);
            }
            
        };
        Function func = null;
        if (abc.getClass().equals(OrcTimestamp.class)) {
            func = timestamp2Int;
        }
        else {
            func = date2Int;
        }
        
        func.apply(abc);
    }
    
    
    // public static class DateColumnVectorWritable 
    //     extends DateColumnVector 
    //     implements WritableComparable<DateColumnVectorWritable> 
    // {
    //     public DateColumnVectorWritable() {
    //         super();
    //     }

    //     public DateColumnVectorWritable(int n) {
    //         super(n);
    //     }
        
    //     @Override
    //     public void write(DataOutput out) throws IOException {
    //         out.writeUTF(SerDeUtil.serialize(vector));
    //     }

    //     @Override
    //     public void readFields(DataInput in) throws IOException {
    //         vector = SerDeUtil.deserialize(in.readUTF(), long[].class);
    //     }

    //     @Override
    //     public int compareTo(DateColumnVectorWritable arg0) {
    //         throw new UnsupportedOperationException("Unimplemented method 'compareTo'");
    //     }
        
    // }
    
    @Test
    public void ColumnVectorSerdeDebug() throws IOException {
        LongColumnVector longcolvec = new LongColumnVector(4096);
        longcolvec.vector[400] = 400L;
        longcolvec.vector[4000] = 4000L;
        LongColumnVector delongcolvec = SerDeUtil.deserialize(SerDeUtil.serialize(longcolvec), LongColumnVector.class);
        System.out.println(delongcolvec.vector[400]);
        System.out.println(delongcolvec.vector[4000]);
        
        // DecimalColumnVector decimalcolvec = new DecimalColumnVector(4096, 10, 2);
        // decimalcolvec.vector[400] = new HiveDecimalWritable("400.4");
        // decimalcolvec.vector[4000] = new HiveDecimalWritable("4000.4");
        // DecimalColumnVector deDecimalColumnVector = SerDeUtil.deserialize(SerDeUtil.serialize(decimalcolvec), DecimalColumnVector.class);
        // System.out.println(deDecimalColumnVector.vector[400]);
        // System.out.println(deDecimalColumnVector.vector[4000]);
        
        // DateColumnVectorWritable dcvw = new DateColumnVectorWritable(4096);
        // dcvw.vector[400] = 400L;
        // dcvw.vector[4000] = 4000L;
        // DataOutputBuffer outputBuffer = new DataOutputBuffer();
        
        // dcvw.write(outputBuffer);
        // DataInputBuffer inputBuffer = new DataInputBuffer();
    }
    
    @Test
    public void timesqldateTimeconvertdebug() {
        Timestamp abc = Timestamp.valueOf("2023-01-01 19:19:19.111999");
        Timestamp wasd = Timestamp.valueOf("2023-01-01 19:19:19.111");
        Timestamp after = Timestamp.valueOf(abc.toLocalDateTime());
        System.out.println(abc.compareTo(wasd));
        System.out.println(after.compareTo(wasd));
    }
    
    @Test
    public void workingDirDebug() {
        System.out.println(System.getProperty("user.dir"));
        System.out.println(new BigDecimal("0.03"));
    }
    
    @Test
    public void DefaultStringifierdebug() throws IOException {
        Configuration conf = new Configuration();
        // DateWritable abc = DefaultStringifier.load(conf, DtlConf.CONTAINS_TIME, DateWritable.class);
        WritableComparable wasd = new DateWritable(0);
        System.out.println(wasd.getClass().getName());
    }
    
    @Test
    public void classForNameAndToString() throws ClassNotFoundException {
        System.out.println(Integer.class.toString());
        System.out.println(Integer.class.getName());
        Class<?> abc = Class.forName(Integer.class.getName());
    }
    
    @Test
    public void kryoSerDe() {
        Configuration conf = new Configuration();
        Kryo kryo = new Kryo();
        // sarg使用kryo
        SearchArgument sarg = SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThan("a_col", PredicateLeaf.Type.LONG, 3L)
            .startNot()
            .lessThanEquals("b_col", PredicateLeaf.Type.LONG, 4L)
            .end()
            .end()
            .build();
        int bufferSize = (int)OrcConf.KRYO_SARG_BUFFER.getLong(conf);
        // Output out = new Output(bufferSize, 16777216);
        // Output out = new Output(bufferSize);
        Output out = new Output(16777216);
        kryo.writeObject(out, sarg);
        OrcConf.KRYO_SARG.setString(conf, Base64.getMimeEncoder().encodeToString(out.toBytes()));
        
        String kryoSarg = OrcConf.KRYO_SARG.getString(conf);
        byte[] sargBytes = Base64.getMimeDecoder().decode(kryoSarg);
        SearchArgument asarg = kryo.readObject(new Input(sargBytes), SearchArgumentImpl.class);
        
        // 如果includedCols使用逗号分隔列名，查询语句里需要给出列名，即select和where和appPrd都需要给出列名
        // 然而appPrd是列号，where没有列名
        boolean[] includedCols = new boolean[]{true, true, false};
        out.clear();
        kryo.writeObject(out, includedCols);
        OrcConf.INCLUDE_COLUMNS.setString(conf, Base64.getMimeEncoder().encodeToString(out.toBytes()));
        
        String includedColStr = OrcConf.INCLUDE_COLUMNS.getString(conf);
        byte[] includedColBytes = Base64.getMimeDecoder().decode(includedColStr);
        boolean[] aincludedCols = kryo.readObject(new Input(includedColBytes), boolean[].class);
        System.out.println();
        
        // 不能序列化反序列化spel的Expression
        // ExpressionParser parser = new SpelExpressionParser();
        // String exprStr = "a_col > 8 && c_col = 14";
        // Expression serExpr = parser.parseExpression(exprStr);
        // String temp = SerDeUtil.serialize(serExpr);
        // Expression deExpr = SerDeUtil.deserialize(temp, Expression.class);
        // System.out.println();
        
        List<IntWritable[]> arraysList = new ArrayList<>();
        arraysList.add(new IntWritable[]{new IntWritable(1), new IntWritable(3), new IntWritable(2)});
        arraysList.add(new IntWritable[]{new IntWritable(3), new IntWritable(2), new IntWritable(1)});
        arraysList.add(new IntWritable[]{new IntWritable(2), new IntWritable(1), new IntWritable(3)});
        String tempArraysList = SerDeUtil.serialize(arraysList);
        List<IntWritable[]> deArraysList = SerDeUtil.deserialize(tempArraysList, ArrayList.class);
        // for (IntWritable[] arr : deArraysList) {
        //     System.out.println(Arrays.toString(arr));
        // }
    }
    
    @Test
    public void testspel() {
        ExpressionParser parser = new SpelExpressionParser();
        Expression expr = parser.parseExpression("'Hello world!'");
        System.out.println(expr.getValue()); // Hello world!
        // static method
        expr = parser.parseExpression("T(java.time.LocalDate).parse('1994-10-24') > T(java.time.LocalDate).parse('1994-10-23')");
        System.out.println(expr.getValue()); // true
        EvaluationContext context = SimpleEvaluationContext.forReadWriteDataBinding().build();
        context.setVariable("discount", BigDecimal.valueOf(0.02));
        expr = parser.parseExpression("#discount > 0.03");
        System.out.println(expr.getValue(context)); // false
        System.out.println(expr.getValueType(context)); // class java.lang.Boolean
        System.out.println(expr.getValueTypeDescriptor(context)); // java.lang.Boolean
        context.setVariable("discount", BigDecimal.valueOf(0.04));
        System.out.println(expr.getValue(context)); // true
        
        // expr = parser.parseExpression("#a_col == (#b_col - 8) && !#c_col || #b_col > 8");
        // context.setVariable("a_col", 3);
        // context.setVariable("b_col", 9);
        // context.setVariable("c_col", false);
        // context.setVariable("d_col", BigDecimal.valueOf(1L));
        // System.out.println(expr.getValue(context)); // true
        
        // expr = parser.parseExpression("#col0 == (#col1 - 8) && !#col2 || #col1 > 8");
        // context.setVariable("col0", 3);
        // context.setVariable("col1", 9);
        // context.setVariable("col2", false);
        // context.setVariable("col3", BigDecimal.valueOf(1L));
        // System.out.println(expr.getValue(context)); // true
        
        // System.out.println(expr.getExpressionString());
        
        // expr = parser.parseExpression("#wonder == 'N'");
        // context.setVariable("wonder", "N");
        // System.out.println(expr.getValue(context));
        // context.setVariable("wonder", "W");
        // System.out.println(expr.getValue(context));
        
        // expr = parser.parseExpression("#NaN > 0.01");
        // context.setVariable("NaN", Double.NaN);
        // System.out.println(expr.getValue(context));
        // expr = parser.parseExpression("#NaN - 0.01");
        // context.setVariable("NaN", Double.NaN);
        // System.out.println(expr.getValue(context));
        // expr = parser.parseExpression("T(java.time.LocalDate).parse('1994-10-24') > #nullDate");
        // context.setVariable("nullDate", null);
        // System.out.println(expr.getValue(context));
        
        // expr = parser.parseExpression("#A + #B");
        // context.setVariable("A", (Object)Long.valueOf(4L));
        // context.setVariable("B", (Object)Long.valueOf(3L));
        // System.out.println(expr.getValue(context));
        
        // 对于非数字的对象支持不好，经常找不到对象的方法
        // expr = parser.parseExpression("#decimalA * (1 - #decimalB)");
        // context.setVariable("decimalA", new BigDecimal("40.55"));
        // context.setVariable("decimalB", new BigDecimal("0.05"));
        // System.out.println(expr.getValue(context));
        // expr = parser.parseExpression("#dateA.isLeapYear()");
        // context.setVariable("dateA", LocalDate.ofEpochDay(100));
        // context.setVariable("dateB", LocalDate.ofEpochDay(1));
        // System.out.println(expr.getValue(context));
        // expr = parser.parseExpression("(#tsA.getTime() - #tsB.getTime()) / 1000.0");
        // context.setVariable("tsA", new Timestamp(1000L));
        // context.setVariable("tsB", new Timestamp(0L));
        // System.out.println(expr.getValue(context));
    }
    
    @Test
    public void subStringBefore() {
        System.out.println(StringUtils.substringBefore("column:3", ":"));
        System.out.println(StringUtils.substringBefore("column", ":"));
        
        
    }
    
    
    @Test
    public void classDebug() {
        System.out.println(List.class.isAssignableFrom(ArrayList.class));
    }

    
    
    @Test
    public void openCsvSerdeDebug() throws IOException {
        int num = 1000000;
        final Text text = new Text("i|123|*");
        long sTime = 0L;
        long eTime = 0L;
        
        // CsvSerde cost -2460
        // CsvParser cost -188
        // 结论：opencsv的实现更适合一行一行的解析
        
        // OpenCSVSerde csv = new OpenCSVSerde();
        // Properties props = new Properties();
        // props.setProperty(serdeConstants.LIST_COLUMNS, "id,num,nonesence");
        // props.setProperty(OpenCSVSerde.SEPARATORCHAR, "|");
        // csv.initialize(null, props, null);
        
        // sTime = System.currentTimeMillis();
        // for (int i = 0; i < num; ++i) {
        //     csv.deserialize(text);
        // }
        // eTime = System.currentTimeMillis();
        // System.out.println("CsvSerde cost " + (sTime - eTime));
        
        // List<String> row = (List<String>)csv.deserialize(text);
        // System.out.println(row);
        
        sTime = System.currentTimeMillis();
        ICSVParser parser = new CSVParserBuilder().withSeparator('|').build();
        for (int i = 0; i < num; ++i) {
            parser.parseLine(text.toString());
        }
        eTime = System.currentTimeMillis();
        System.out.println("CsvParser cost " + (sTime - eTime));
    }
}
