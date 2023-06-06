package xyz.mfj.dataQuery;

import org.apache.hadoop.io.WritableComparable;

public interface Aggregator {
    /**
     * 将一个数据收集到聚合函数的辅助结构中，对源表中的每一行应用一次
     * @param value 表在某行某列的一个数据
     * @param flag 有效时间起止实践标记，0为起始时间，1为终止时间
     */
    // public void collect(WritableComparable value, int flag);
    public void collect(Object value, int flag);
    
    /**
     * 聚合算法后处理并返回一个聚合结果数据
     * @return 一个聚合结果数据
     */
    // public WritableComparable aggregate();
    public Object aggregate();
    
    // agg重置到初始状态
    public void reset();
}
