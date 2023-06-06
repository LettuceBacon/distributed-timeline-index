package xyz.mfj.dataQuery;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;

import xyz.mfj.dataDefiniation.RowVector;

public class DtlExpression implements Writable {
    private String[] colNames;
    private int[] includedColIds;
    private Expression expr;
    private EvaluationContext context;
    
    public DtlExpression() {}
    
    public DtlExpression(String exprStr, int[] includedColIds, String[] colNames) {
        ExpressionParser parser = new SpelExpressionParser();
        this.expr = parser.parseExpression(exprStr);
        this.context = SimpleEvaluationContext.forReadWriteDataBinding().build();
        this.includedColIds = includedColIds;
        this.colNames = colNames;
    }
    
    public int[] getIncludedColIds() {
        return includedColIds;
    }
    
    public Object evaluate(RowVector row) {
        for (int i = 0; i < includedColIds.length; ++i) {
            Object operand = row.get(includedColIds[i]);
            if (operand == null) {
                return null;
            }
            context.setVariable(colNames[i], operand);
        }
        return expr.getValue(context);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(expr.getExpressionString());
        out.writeInt(includedColIds.length);
        for (int i = 0; i < includedColIds.length; ++i) {
            out.writeInt(includedColIds[i]);
            out.writeUTF(colNames[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ExpressionParser parser = new SpelExpressionParser();
        expr = parser.parseExpression(in.readUTF());
        context = SimpleEvaluationContext.forReadWriteDataBinding().build();
        int len = in.readInt();
        includedColIds = new int[len];
        colNames = new String[len];
        for (int i = 0; i < len; ++i) {
            includedColIds[i] = in.readInt();
            colNames[i] = in.readUTF();
        }
    }
}
