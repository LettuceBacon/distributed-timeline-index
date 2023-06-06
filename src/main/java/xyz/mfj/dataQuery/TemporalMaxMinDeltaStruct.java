package xyz.mfj.dataQuery;

import java.util.ArrayList;

import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcStruct;

import xyz.mfj.dataDefiniation.TimelineIndex;
import xyz.mfj.utils.TypeUtil;

public class TemporalMaxMinDeltaStruct implements DeltaStruct {
    // 数据是select.eval()的结果
    // 在一个时间上变有效的数据集
    private ArrayList<Object> becomeValidSet;
    // 在一个时间上变无效的数据集
    private ArrayList<Object> becomeInvalidSet;
    
    public TemporalMaxMinDeltaStruct() {
        this.becomeValidSet = new ArrayList<>();
        this.becomeInvalidSet = new ArrayList<>();
    }
    
    @Override
    public void storeDeltaValue(int flag, Object value) {
        if (value == null) {
            return;
        }
        if (flag == TimelineIndex.STARTIDX) {
            becomeValidSet.add(value);
        }
        else if (flag == TimelineIndex.ENDIDX) {
            becomeInvalidSet.add(value);
        }
        else {
            throw new IllegalArgumentException("Illegal flag");
        }
    }

    @Override
    public void convertTo(OrcStruct shuffleDeltaValues, int fieldId) {
        OrcStruct sets = (OrcStruct)shuffleDeltaValues.getFieldValue(fieldId);
        OrcList shuffleBecomeValidSet = (OrcList)sets.getFieldValue(0);
        shuffleBecomeValidSet.clear();
        for (Object value : becomeValidSet) {
            shuffleBecomeValidSet.add(TypeUtil.jObj2hadoopObj(value));
        }
        OrcList shuffleBecomeInvalidSet = (OrcList)sets.getFieldValue(1);
        shuffleBecomeInvalidSet.clear();
        for (Object value : becomeInvalidSet) {
            shuffleBecomeInvalidSet.add(TypeUtil.jObj2hadoopObj(value));
        }
    }
}
