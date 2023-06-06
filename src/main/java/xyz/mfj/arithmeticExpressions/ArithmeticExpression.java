package xyz.mfj.arithmeticExpressions;

import java.util.HashMap;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;

public abstract class ArithmeticExpression {
    public static enum ArithOprEnum {
        ADD,MINUS,MULTIPLY,DIVIDE,MOD;
    }

    // protected TypeDescription aType;
    // protected TypeDescription bType;
    // protected ArithOprEnum Opr;
    private static HashMap<
        Triple<TypeDescription.Category, TypeDescription.Category, ArithOprEnum>, 
        ArithmeticExpression
    > ArithExprMap = new HashMap<>();
    static {
        ArithExprMap.put(
            Triple.of(TypeDescription.Category.LONG, 
                TypeDescription.Category.LONG, ArithOprEnum.ADD), 
            new LongLongAdd()
        );
        ArithExprMap.put(
            Triple.of(TypeDescription.Category.LONG, 
                TypeDescription.Category.LONG, ArithOprEnum.MINUS), 
            new LongLongMinus()
        );
        ArithExprMap.put(
            Triple.of(TypeDescription.Category.INT, 
                TypeDescription.Category.INT, ArithOprEnum.ADD), 
            new IntIntAdd()
        );
        ArithExprMap.put(
            Triple.of(TypeDescription.Category.INT, 
                TypeDescription.Category.INT, ArithOprEnum.MINUS), 
            new IntIntMinus()
        );
        ArithExprMap.put(
            Triple.of(TypeDescription.Category.DECIMAL, 
                TypeDescription.Category.DECIMAL, ArithOprEnum.ADD), 
            new DecimalDecimalAdd()
        );
        ArithExprMap.put(
            Triple.of(TypeDescription.Category.DECIMAL, 
                TypeDescription.Category.DECIMAL, ArithOprEnum.MINUS), 
            new DecimalDecimalMinus()
        );
        ArithExprMap.put(
            Triple.of(TypeDescription.Category.DOUBLE, 
                TypeDescription.Category.DOUBLE, ArithOprEnum.ADD), 
            new DoubleDoubleAdd()
        );
        ArithExprMap.put(
            Triple.of(TypeDescription.Category.DOUBLE, 
                TypeDescription.Category.DOUBLE, ArithOprEnum.MINUS), 
            new DoubleDoubleMinus()
        );
    }
    
    public static ArithmeticExpression getExpr(
        TypeDescription.Category aType, 
        TypeDescription.Category bType, 
        ArithOprEnum opr
    ) {
        return ArithExprMap.get(Triple.of(aType, bType, opr));
    }
    
    // Hadoop类型系统值的计算
    public abstract WritableComparable eval(WritableComparable a, WritableComparable b);
    
    // java类型系统值的计算
    public abstract Object eval(Object a, Object b);
}
