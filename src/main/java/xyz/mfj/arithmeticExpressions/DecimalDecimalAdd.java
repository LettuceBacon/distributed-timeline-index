package xyz.mfj.arithmeticExpressions;

import java.math.BigDecimal;

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.WritableComparable;

public class DecimalDecimalAdd extends ArithmeticExpression{

    @Override
    public WritableComparable eval(WritableComparable a, WritableComparable b) {
        return new HiveDecimalWritable(
            ((HiveDecimalWritable)a).getHiveDecimal()
            .add(
            ((HiveDecimalWritable)b).getHiveDecimal())
        );
    }

    @Override
    public Object eval(Object a, Object b) {
        return ((BigDecimal)a).add((BigDecimal)b);
    }
    
}
