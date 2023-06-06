package xyz.mfj.arithmeticExpressions;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntIntAdd extends ArithmeticExpression{

    @Override
    public WritableComparable eval(WritableComparable a, WritableComparable b) {
        return new IntWritable(((IntWritable)a).get() + ((IntWritable)b).get());
    }

    @Override
    public Object eval(Object a, Object b) {
        return (Integer)a + (Integer)b;
    }
    
}
