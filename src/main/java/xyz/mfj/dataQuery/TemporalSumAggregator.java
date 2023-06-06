package xyz.mfj.dataQuery;


import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.arithmeticExpressions.ArithmeticExpression;
import xyz.mfj.arithmeticExpressions.ArithmeticExpression.ArithOprEnum;
import xyz.mfj.dataDefiniation.TimelineIndex;
import xyz.mfj.utils.TypeUtil;

public class TemporalSumAggregator implements Aggregator {
    private static Logger log = LoggerFactory.getLogger(TemporalSumAggregator.class);
    
    private TypeDescription type;
    private Object sum;
    private ArithmeticExpression add;
    private ArithmeticExpression minus;
    
    public TemporalSumAggregator(TypeDescription type) {
        if (!TypeUtil.isNumeric(type)) {
            log.error("Temporal SUM() can only apply on numeric type!");
        }
        this.type = type;
        TypeDescription.Category category = type.getCategory();
        this.sum = TypeUtil.zeroOf(type);
        this.add = ArithmeticExpression.getExpr(category, category, ArithOprEnum.ADD);
        this.minus = ArithmeticExpression.getExpr(category, category, ArithOprEnum.MINUS);
    }

    @Override
    public Object aggregate() {
        return sum;
    }

    @Override
    public void collect(Object value, int flag) {
        if (value == null) {
            return;
        }
        if (flag == TimelineIndex.STARTIDX) {
            sum = add.eval(sum, value);
        }
        else if (flag == TimelineIndex.ENDIDX) {
            sum = minus.eval(sum, value);
        }
        else {
            throw new IllegalArgumentException("Illegal flag in SUM()");
        }
    }

    @Override
    public void reset() {
        this.sum = TypeUtil.zeroOf(type);
    }
    
}
