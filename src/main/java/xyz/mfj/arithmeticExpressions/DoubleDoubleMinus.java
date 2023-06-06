package xyz.mfj.arithmeticExpressions;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class DoubleDoubleMinus extends ArithmeticExpression {
    @Override
    public WritableComparable eval(WritableComparable a, WritableComparable b) {
        return new DoubleWritable(((DoubleWritable)a).get() - ((DoubleWritable)b).get());
    }

    @Override
    public Object eval(Object a, Object b) {
        return (Double)a - (Double)b;
    }
}
