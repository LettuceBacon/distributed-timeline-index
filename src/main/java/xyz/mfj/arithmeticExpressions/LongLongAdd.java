package xyz.mfj.arithmeticExpressions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class LongLongAdd extends ArithmeticExpression{
    @Override
    public WritableComparable eval(WritableComparable a, WritableComparable b) {
        return new LongWritable(((LongWritable)a).get() + ((LongWritable)b).get());
    }

    @Override
    public Object eval(Object a, Object b) {
        return (Long)a + (Long)b;
    }
}
