package xyz.mfj.dataQuery;

import java.util.LinkedList;
import java.util.TreeMap;

import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.dataDefiniation.TimelineIndex;
import xyz.mfj.utils.TypeUtil;

// HiveDecimalWritable 类型的数据无法通过TreeMap::put加入TreeMap，只能通过TreeMap::putAll加入
public class TemporalMinAggregator implements Aggregator{
    private static Logger log = LoggerFactory.getLogger(TemporalMinAggregator.class);
    
    private TypeDescription type;
    private TopKSet topKSet; // 内部实现是一个从小到大排序的java.util.TreeMap
    // private LinkedList<WritableComparable> validVector;
    // private LinkedList<WritableComparable> deletedVector;
    private LinkedList<Comparable> validVector;
    private LinkedList<Comparable> deletedVector;
    
    public TemporalMinAggregator(TypeDescription type) {
        this.type = type;
        this.topKSet = new TopKSet();
        this.validVector = new LinkedList<>();
        this.deletedVector = new LinkedList<>();
        this.topKSet.insert((Comparable)TypeUtil.maxOf(type));
        // this.topKSet.insert(TypeUtil.maxOf(type));
    }
    
    @Override
    public void collect(Object val, int flag) {
        if (val == null || val.equals(TypeUtil.nullOf(type))) {
            return;
        }
        Comparable value = (Comparable)val;
        if (flag == TimelineIndex.STARTIDX) {
            if (topKSet.size() == 0 || value.compareTo(topKSet.getMaxValue()) <= 0) { // 小于或等于topk集最大值
                Comparable valueKPlusOne = topKSet.insert(value);
                if (valueKPlusOne != null) { // 插入topk集后多出一个
                    validVector.add(valueKPlusOne);
                }
            }
            else {
                validVector.add(value);
            }
        }
        else if (flag == TimelineIndex.ENDIDX) {
            if (topKSet.containsValue(value)) {
                topKSet.deleteAndGet(value);
                while (topKSet.size() <= 0) {
                    rebuildTopKSet();
                }
            }
            else {
                deletedVector.add(value);
            }
        }
    }
    
    // @Override
    // public void collect(WritableComparable value, int flag) {
    //     if (flag == TimelineIndex.STARTIDX) {
    //         if (topKSet.size() == 0 || value.compareTo(topKSet.getMaxValue()) <= 0) { // 小于或等于topk集最大值
    //             WritableComparable valueKPlusOne = topKSet.insert(value);
    //             if (valueKPlusOne != null) { // 插入topk集后多出一个
    //                 validVector.add(valueKPlusOne);
    //             }
    //         }
    //         else {
    //             validVector.add(value);
    //         }
    //     }
    //     else if (flag == TimelineIndex.ENDIDX) {
    //         if (topKSet.containsValue(value)) {
    //             topKSet.deleteAndGet(value);
    //             while (topKSet.size() <= 0) {
    //                 rebuildTopKSet();
    //             }
    //         }
    //         else {
    //             deletedVector.add(value);
    //         }
    //     }
    // }

    @Override
    public Object aggregate() {
        return topKSet.getMinValue();
    }
    
    // @Override
    // public WritableComparable aggregate() {
    //     return topKSet.getMinValue();
    // }
    
    private void rebuildTopKSet() {
        int validVectorSize = validVector.size();
        int deletedVectorSize = deletedVector.size();
        for (int i = 0; i < validVectorSize; i++) {
            Comparable valueKPlusOne = topKSet.insert(validVector.remove());
            if (valueKPlusOne != null) { // 插入topk集后多出一个
                validVector.add(valueKPlusOne);
            }
        }
        for (int i = 0; i < deletedVectorSize; i++) {
            Comparable value = deletedVector.remove();
            if (topKSet.containsValue(value)) {
                topKSet.deleteAndGet(value);
            }
            else {
                deletedVector.add(value);
            }
        }
    }
    
    
    private class TopKSet {
        private TreeMap<Comparable, Integer> ascendOrderTree;
        private int k = 100;
        private int size = 0;
        
        public TopKSet(int k) {
            this.ascendOrderTree = new TreeMap<>();
            this.k = k;
        }
        
        public TopKSet() {
            this.ascendOrderTree = new TreeMap<>();
        }
        
        public Comparable getMinValue() {
            return ascendOrderTree.firstKey();
        }
        
        public Comparable getMaxValue() {
            return ascendOrderTree.lastKey();
        }
        
        public Comparable insert(Comparable value) {
            Integer oldCount = ascendOrderTree.get(value);
            if (oldCount == null) { // 树中没有对应的值
                ascendOrderTree.put(value, 1);
            }
            else {
                ascendOrderTree.put(value, oldCount + 1);
            }
            size++;
            if (size > k) { // 插入topk集后多出一个，从树中拿出一个最大值并返回
                return deleteAndGet(getMaxValue());
            }
            else {
                return null;
            }
        }
        
        public boolean containsValue(Comparable value) {
            return ascendOrderTree.containsKey(value);
        }
        
        public Comparable deleteAndGet(Comparable value) {
            Integer oldCount = ascendOrderTree.get(value);
            if (oldCount == null) { 
                log.error("Deleting an unexisted value from topK set!");
                return null;
            }
            else {
                if (oldCount - 1 > 0) {
                    ascendOrderTree.put(value, oldCount - 1);
                }
                else {
                    ascendOrderTree.remove(value);
                }
                size--;
                return value;
            }
        }
        
        public int size() {
            return size;
        }
    }

    // private class TopKSet {
    //     private TreeMap<WritableComparable, Integer> ascendOrderTree;
    //     private int k = 100;
    //     private int size = 0;
        
    //     public TopKSet(int k) {
    //         this.ascendOrderTree = new TreeMap<>();
    //         this.k = k;
    //     }
        
    //     public TopKSet() {
    //         this.ascendOrderTree = new TreeMap<>();
    //     }
        
    //     public WritableComparable getMinValue() {
    //         return ascendOrderTree.firstKey();
    //     }
        
    //     public WritableComparable getMaxValue() {
    //         return ascendOrderTree.lastKey();
    //     }
        
    //     public WritableComparable insert(WritableComparable value) {
    //         Integer oldCount = ascendOrderTree.get(value);
    //         if (oldCount == null) { // 树中没有对应的值
    //             ascendOrderTree.put(value, 1);
    //         }
    //         else {
    //             ascendOrderTree.put(value, oldCount + 1);
    //         }
    //         size++;
    //         if (size > k) { // 插入topk集后多出一个，从树中拿出一个最大值并返回
    //             return deleteAndGet(getMaxValue());
    //         }
    //         else {
    //             return null;
    //         }
    //     }
        
    //     public boolean containsValue(WritableComparable value) {
    //         return ascendOrderTree.containsKey(value);
    //     }
        
    //     public WritableComparable deleteAndGet(WritableComparable value) {
    //         Integer oldCount = ascendOrderTree.get(value);
    //         if (oldCount == null) { 
    //             log.error("Deleting an unexisted value from topK set!");
    //             return null;
    //         }
    //         else {
    //             if (oldCount - 1 > 0) {
    //                 ascendOrderTree.put(value, oldCount - 1);
    //             }
    //             else {
    //                 ascendOrderTree.remove(value);
    //             }
    //             size--;
    //             return value;
    //         }
    //     }
        
    //     public int size() {
    //         return size;
    //     }
    // }

    @Override
    public void reset() {
        this.topKSet = new TopKSet();
        this.validVector = new LinkedList<>();
        this.deletedVector = new LinkedList<>();
        // this.topKSet.insert(TypeUtil.maxOf(type));
        this.topKSet.insert((Comparable)TypeUtil.maxOf(type));
    }
}
