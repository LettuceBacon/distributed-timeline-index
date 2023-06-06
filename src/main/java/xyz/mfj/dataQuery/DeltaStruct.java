package xyz.mfj.dataQuery;

import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

// delta value 的父类，每个delta value对应一个select子句
public interface DeltaStruct {
    public static DeltaStruct[] createDeltaValues(Text[] aggregatorNames, TypeDescription deltaValuesSchema) {
        DeltaStruct[] deltaValues = new DeltaStruct[aggregatorNames.length];
        List<TypeDescription> children = deltaValuesSchema.getChildren();
        for (int i = 0; i < aggregatorNames.length; ++i) {
            switch (aggregatorNames[i].toString().toLowerCase()) {
                case "sum":
                    deltaValues[i] = new TemporalSumDeltaStruct(children.get(i));
                    break;
            
                case "max":
                case "min":
                    deltaValues[i] = new TemporalMaxMinDeltaStruct();
                    break;
                
                default:
                    throw new UnsupportedOperationException(
                        String.format("Unimplemented aggregator '%s'", aggregatorNames[i].toString())
                    );
            }
        }
        return deltaValues;
    }
    
    
    public void storeDeltaValue(int flag, Object value);
    public void convertTo(OrcStruct shuffleDeltaValues, int fieldId);
}
