package xyz.mfj.dataQuery;

import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;

import xyz.mfj.dataDefiniation.TimelineIndex;
import xyz.mfj.utils.TypeUtil;

public class TemporalSumDeltaStruct implements DeltaStruct {
    private static final String DELTAVALUE = "deltaValue";
    private static final String VALUE = "value";
    private static Expression addExpr;
    private static Expression minusExpr;
    private static EvaluationContext context;
    
    static {
        ExpressionParser parser = new SpelExpressionParser();
        addExpr = parser.parseExpression("#deltaValue + #value");
        minusExpr = parser.parseExpression("#deltaValue - #value");
        context = SimpleEvaluationContext.forReadWriteDataBinding().build();
    }
    
    private Object deltaValue;
    
    public TemporalSumDeltaStruct(TypeDescription type) {
        deltaValue = TypeUtil.zeroOf(type);
    }

    @Override
    public void storeDeltaValue(int flag, Object value) {
        if (value == null) {
            return;
        }
        // if (deltaValue == null) {
        //     deltaValue = value;
        //     return;
        // }
        context.setVariable(DELTAVALUE, deltaValue);
        context.setVariable(VALUE, value);
        if (flag == TimelineIndex.STARTIDX) {
            deltaValue = addExpr.getValue(context);
        }
        else if (flag == TimelineIndex.ENDIDX) {
            deltaValue = minusExpr.getValue(context);
        }
        else {
            throw new IllegalArgumentException("Illegal flag");
        }
    }

    @Override
    public void convertTo(OrcStruct shuffleDeltaValues, int fieldId) {
        shuffleDeltaValues.setFieldValue(fieldId, TypeUtil.jObj2hadoopObj(deltaValue));
    }
}
