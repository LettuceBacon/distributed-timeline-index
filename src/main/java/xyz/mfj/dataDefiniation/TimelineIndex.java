package xyz.mfj.dataDefiniation;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.min;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import net.sourceforge.sizeof.SizeOf;
import uk.co.omegaprime.btreemap.BTreeMap;
import xyz.mfj.enhanced.ColumnVectorEnhance;
import xyz.mfj.enhanced.TypeDescriptionEnhance;
import xyz.mfj.enhanced.ColumnVectorEnhance.ElemGetter;

public class TimelineIndex {
    private static final Logger LOG = LoggerFactory.getLogger(TimelineIndex.class);
    public static final String RF0 = "RF0";
    public static final String RF1 = "RF1";
    public static final int STARTIDX = 0;
    public static final int ENDIDX = 1;

    public TimelineIndex() {
    }

    public void buildIndex(VectorizedRowBatch rowBatch,
            TypeDescription batchSchema,
            ApplicationPeriod appPrd
    ) {
        // quickSortBuildIndex(rowBatch, batchSchema, appPrd);
        
        InfixCountingSortBuildIndex(rowBatch, batchSchema, appPrd);
        // bTreeSortBuildIndex(rowBatch, batchSchema, appPrd);
        // countSortBuildIndex(rowBatch, batchSchema, appPrd);
    }

    public static class VRF implements Comparable<VRF> {
        private Comparable version;
        // private WritableComparable version;
        private int rowId;
        private int flag;

        public VRF() {
        }

        public VRF(Comparable version, int rowId, int flag) {
            // public VRF(WritableComparable version, int rowId, int flag) {
            this.version = version;
            this.rowId = rowId;
            this.flag = flag;
        }

        public Comparable getVersion() {
            // public WritableComparable getVersion() {
            return version;
        }

        public int getRowId() {
            return rowId;
        }

        public int getFlag() {
            return flag;
        }

        public void setVersion(Comparable v) {
            // public void setVersion(WritableComparable v) {
            version = v;
        }

        public long toRF() {
            return (rowId << 1) + flag;
        }

        public void fromRF(long rf) {
            rowId = (int) (rf >> 1);
            flag = (int) (rf % 2L);
        }

        @Override
        public int compareTo(VRF arg0) {
            int result = version.compareTo(arg0.getVersion());
            if (result == 0) {
                result = rowId - arg0.rowId;
            }
            return result;
        }
    }

    private static class TimelineIndexIterator implements Iterator<VRF> {
        private ColumnVector[] V;
        private LongColumnVector[] RF;
        private int maxSize;
        private int i;
        private ElemGetter getter;

        private TimelineIndexIterator(ApplicationPeriod appPrd,
                VectorizedRowBatch rowBatch,
                TypeDescription batchSchema) {
            TypeDescription VType = TypeDescriptionEnhance
                    .getNameAndType(batchSchema, appPrd.getAppPrdSId() + 1)
                    .getRight();
            this.V = new ColumnVector[] {
                    rowBatch.cols[appPrd.getAppPrdSId()],
                    rowBatch.cols[appPrd.getAppPrdEId()]
            };
            this.RF = new LongColumnVector[] {
                    (LongColumnVector) rowBatch.cols[appPrd.getRf0Id()],
                    (LongColumnVector) rowBatch.cols[appPrd.getRf1Id()]
            };
            this.maxSize = rowBatch.size * 2;
            this.i = 0;
            this.getter = ColumnVectorEnhance.createElemGetters(VType)[0];
        }

        @Override
        public boolean hasNext() {
            return i < maxSize;
        }

        @Override
        public VRF next() {
            VRF next = new VRF();
            next.fromRF(RF[i % 2].vector[i >> 1]);
            next.setVersion((Comparable) getter.getElem(V[next.getFlag()], next.getRowId()));
            i++;
            return next;
        }
    }

    public Iterator<VRF> iterator(ApplicationPeriod appPrd,
            VectorizedRowBatch rowBatch,
            TypeDescription batchSchema) {
        return new TimelineIndexIterator(appPrd, rowBatch, batchSchema);
    }

    private void quickSortBuildIndex(VectorizedRowBatch rowBatch,
            TypeDescription batchSchema,
            ApplicationPeriod appPrd
    ) {
        
        long sTime = System.currentTimeMillis();

        TypeDescription VType = TypeDescriptionEnhance
                .getNameAndType(batchSchema, appPrd.getAppPrdSId() + 1)
                .getRight();
        VRF[] vrfArray = new VRF[rowBatch.size * 2];

        ElemGetter getter = ColumnVectorEnhance.createElemGetters(VType)[0];
        ColumnVector appPrdStart = rowBatch.cols[appPrd.getAppPrdSId()];
        ColumnVector appPrdEnd = rowBatch.cols[appPrd.getAppPrdEId()];
        for (int row = 0; row < rowBatch.size; row++) {
            Comparable startV = (Comparable) getter.getElem(appPrdStart, row);
            Comparable endV = (Comparable) getter.getElem(appPrdEnd, row);
            if (startV == null || endV == null) {
                throw new NullPointerException("Period end point has null value!");
            }
            if (startV.compareTo(endV) >= 0) {
                LOG.error("Period start point should be earlier than period end point!");
                System.exit(1);
            }
            vrfArray[row << 1] = new VRF(startV, row, STARTIDX);
            vrfArray[(row << 1) + 1] = new VRF(endV, row, ENDIDX);
            // vrfArray[row << 1] = new VRF(getter.getHadoopElem(appPrdStart, row), row,
            // STARTIDX);
            // vrfArray[(row << 1) + 1] = new VRF(getter.getHadoopElem(appPrdEnd, row), row,
            // ENDIDX);
        }

        Arrays.sort(vrfArray);
        // for (int i = 0; i < 20; i++) {
        // LOG.info("{}", vrfArray[i].getVersion());
        // }

        LongColumnVector RF0 = (LongColumnVector) rowBatch.cols[appPrd.getRf0Id()];
        LongColumnVector RF1 = (LongColumnVector) rowBatch.cols[appPrd.getRf1Id()];
        for (int row = 0; row < rowBatch.size; row++) {
            RF0.vector[row] = vrfArray[row << 1].toRF();
            RF1.vector[row] = vrfArray[(row << 1) + 1].toRF();
        }

        long eTime = System.currentTimeMillis();
        LOG.info("Build timeline index cost {} ms", eTime - sTime);
    }

    private void InfixCountingSortBuildIndex(VectorizedRowBatch rowBatch,
            TypeDescription batchSchema,
            ApplicationPeriod appPrd
    ) {
        
        long sTime = System.currentTimeMillis();

        TypeDescription VType = TypeDescriptionEnhance
                .getNameAndType(batchSchema, appPrd.getAppPrdSId() + 1)
                .getRight();
        Function<Comparable, Long> version2Long = 
            VType.getCategory().equals(TypeDescription.Category.DATE)
                ? new Function<Comparable, Long>() {

                    @Override
                    public Long apply(Comparable arg0) {
                        return ((LocalDate)((VRF)arg0).getVersion()).toEpochDay();
                    }

                }
                : new Function<Comparable, Long>() {

                    @Override
                    public Long apply(Comparable arg0) {
                        return ((Timestamp)((VRF)arg0).getVersion()).getTime();
                    }

                };
        ElemGetter getter = ColumnVectorEnhance.createElemGetters(VType)[0];
        ColumnVector appPrdStart = rowBatch.cols[appPrd.getAppPrdSId()];
        ColumnVector appPrdEnd = rowBatch.cols[appPrd.getAppPrdEId()];
        VRF[] vrfArray = new VRF[rowBatch.size * 2];
        for (int row = 0; row < rowBatch.size; row++) {
            Comparable startV = (Comparable) getter.getElem(appPrdStart, row);
            Comparable endV = (Comparable) getter.getElem(appPrdEnd, row);
            if (startV == null || endV == null) {
                throw new NullPointerException("Period end point has null value!");
            }
            if (startV.compareTo(endV) >= 0) {
                LOG.error("Period start point should be earlier than period end point!");
                System.exit(1);
            }
            vrfArray[row << 1] = new VRF(startV, row, STARTIDX);
            vrfArray[(row << 1) + 1] = new VRF(endV, row, ENDIDX);
            // vrfArray[row << 1] = new VRF(getter.getHadoopElem(appPrdStart, row), row,
            // STARTIDX);
            // vrfArray[(row << 1) + 1] = new VRF(getter.getHadoopElem(appPrdEnd, row), row,
            // ENDIDX);
        }
        
        InfixCountingSort.sort(vrfArray, version2Long);
        
        LongColumnVector RF0 = (LongColumnVector) rowBatch.cols[appPrd.getRf0Id()];
        LongColumnVector RF1 = (LongColumnVector) rowBatch.cols[appPrd.getRf1Id()];
        for (int row = 0; row < rowBatch.size; row++) {
            RF0.vector[row] = vrfArray[row << 1].toRF();
            RF1.vector[row] = vrfArray[(row << 1) + 1].toRF();
        }
        
        long eTime = System.currentTimeMillis();
        LOG.info("Build timeline index cost {} ms", eTime - sTime);
    }
    
    private void bTreeSortBuildIndex(VectorizedRowBatch rowBatch,
        TypeDescription batchSchema,
        ApplicationPeriod appPrd
    ) {
        // btree
        // Build timeline index cost 1074 ms, with assist struct 78077720 bytes
        // Build timeline index cost 777 ms, with assist struct 78077720 bytes
        // Build timeline index cost 886 ms, with assist struct 78077720 bytes
        
        // rbtree
        // Build timeline index cost 1039 ms
        // Build timeline index cost 797 ms
        // Build timeline index cost 856 ms
        // Assist struct 105700736 bytes
        
        long sTime = System.currentTimeMillis();

        TypeDescription VType = TypeDescriptionEnhance
                .getNameAndType(batchSchema, appPrd.getAppPrdSId() + 1)
                .getRight();
        VRF[] vrfArray = new VRF[rowBatch.size * 2];

        ElemGetter getter = ColumnVectorEnhance.createElemGetters(VType)[0];
        ColumnVector appPrdStart = rowBatch.cols[appPrd.getAppPrdSId()];
        ColumnVector appPrdEnd = rowBatch.cols[appPrd.getAppPrdEId()];
        for (int row = 0; row < rowBatch.size; row++) {
            Comparable startV = (Comparable) getter.getElem(appPrdStart, row);
            Comparable endV = (Comparable) getter.getElem(appPrdEnd, row);
            if (startV == null || endV == null) {
                throw new NullPointerException("Period end point has null value!");
            }
            if (startV.compareTo(endV) >= 0) {
                LOG.error("Period start point should be earlier than period end point!");
                System.exit(1);
            }
            vrfArray[row << 1] = new VRF(startV, row, STARTIDX);
            vrfArray[(row << 1) + 1] = new VRF(endV, row, ENDIDX);
            // vrfArray[row << 1] = new VRF(getter.getHadoopElem(appPrdStart, row), row,
            // STARTIDX);
            // vrfArray[(row << 1) + 1] = new VRF(getter.getHadoopElem(appPrdEnd, row), row,
            // ENDIDX);
        }

        BTreeMap<VRF, Integer> bpt = BTreeMap.create();
        for (int i = 0; i < vrfArray.length; ++i) {
            bpt.put(vrfArray[i], null);
        }
        int vrfId = 0;
        for (VRF vrf : bpt.navigableKeySet()) {
            vrfArray[vrfId++] = vrf;
        }
        
        // TreeMap<VRF, Integer> bpt = new TreeMap<>();
        // for (int i = 0; i < vrfArray.length; ++i) {
        //     bpt.put(vrfArray[i], null);
        // }
        // int vrfId = 0;
        // for (VRF vrf : bpt.navigableKeySet()) {
        //     vrfArray[vrfId++] = vrf;
        // }
        
        LongColumnVector RF0 = (LongColumnVector) rowBatch.cols[appPrd.getRf0Id()];
        LongColumnVector RF1 = (LongColumnVector) rowBatch.cols[appPrd.getRf1Id()];
        for (int row = 0; row < rowBatch.size; row++) {
            RF0.vector[row] = vrfArray[row << 1].toRF();
            RF1.vector[row] = vrfArray[(row << 1) + 1].toRF();
        }

        long eTime = System.currentTimeMillis();
        LOG.info("Build timeline index cost {} ms", eTime - sTime);
        // LOG.info("Assist struct {} bytes", SizeOf.deepSizeOf(bpt));
    }
    
    private void countSortBuildIndex(VectorizedRowBatch rowBatch,
        TypeDescription batchSchema,
        ApplicationPeriod appPrd
    ) {
        // Build timeline index cost 180 ms, with assist struct 20448 bytes
        // Build timeline index cost 151 ms, with assist struct 20448 bytes
        // Build timeline index cost 143 ms, with assist struct 20448 bytes
        
        long sTime = System.currentTimeMillis();

        TypeDescription VType = TypeDescriptionEnhance
                .getNameAndType(batchSchema, appPrd.getAppPrdSId() + 1)
                .getRight();
        Function<Comparable, Long> version2Long = 
            VType.getCategory().equals(TypeDescription.Category.DATE)
                ? new Function<Comparable, Long>() {

                    @Override
                    public Long apply(Comparable arg0) {
                        return ((LocalDate)arg0).toEpochDay();
                    }

                }
                : new Function<Comparable, Long>() {

                    @Override
                    public Long apply(Comparable arg0) {
                        return ((Timestamp)arg0).getTime();
                    }

                };
                
        ElemGetter getter = ColumnVectorEnhance.createElemGetters(VType)[0];
        ColumnVector appPrdStart = rowBatch.cols[appPrd.getAppPrdSId()];
        ColumnVector appPrdEnd = rowBatch.cols[appPrd.getAppPrdEId()];
        long minDigitKey = Long.MAX_VALUE;
        long maxDigitKey = 0L;
        // 读一遍rowbatch拿到version数值型最小值最大值，
        for (int row = 0; row < rowBatch.size; row++) {
            Comparable startV = (Comparable) getter.getElem(appPrdStart, row);
            Comparable endV = (Comparable) getter.getElem(appPrdEnd, row);
            if (startV == null || endV == null) {
                throw new NullPointerException("Period end point has null value!");
            }
            if (startV.compareTo(endV) >= 0) {
                LOG.error("Period start point should be earlier than period end point!");
                System.exit(1);
            }
            long digitKey = version2Long.apply(startV);
            if (digitKey < minDigitKey) minDigitKey = digitKey;
            if (digitKey > maxDigitKey) maxDigitKey = digitKey;
            digitKey = version2Long.apply(endV);
            if (digitKey < minDigitKey) minDigitKey = digitKey;
            if (digitKey > maxDigitKey) maxDigitKey = digitKey;
        }
        
        // 构建一个大小为最大值最小值之差的计数数组
        int[] counterArray = new int[(int)(maxDigitKey - minDigitKey + 1L)];
        // 再读一遍rowbatch，将version数值型放入计数数组
        for (int row = 0; row < rowBatch.size; row++) {
            long digitKey = version2Long.apply(((Comparable)getter.getElem(appPrdStart, row)));
            counterArray[(int)(digitKey - minDigitKey)]++;
            digitKey = version2Long.apply(((Comparable)getter.getElem(appPrdEnd, row)));
            counterArray[(int)(digitKey - minDigitKey)]++;
        }
        
        // 读计数数组，构造累积计数数组
        int[] cumulationArray = new int[(int)(maxDigitKey - minDigitKey + 1L)];
        for (int i = 1; i < counterArray.length; ++i) {
            cumulationArray[i] = cumulationArray[i - 1] + counterArray[i - 1];
        }
        
        // 再读一遍rowbatch，构造vrf并根据计数数组标记的位置放入vrfArray
        VRF[] vrfArray = new VRF[rowBatch.size * 2];
        for (int row = 0; row < rowBatch.size; row++) {
            VRF vrf = new VRF((Comparable)getter.getElem(appPrdStart, row), row, STARTIDX);
            long digitKey = version2Long.apply(vrf.getVersion());
            vrfArray[cumulationArray[(int)(digitKey - minDigitKey)]++] = vrf;
            vrf = new VRF((Comparable)getter.getElem(appPrdEnd, row), row, ENDIDX);
            digitKey = version2Long.apply(vrf.getVersion());
            vrfArray[cumulationArray[(int)(digitKey - minDigitKey)]++] = vrf;
        }
        
        // 用Arrays.sort()处理一遍vrfArray
        Arrays.sort(vrfArray);
        
        LongColumnVector RF0 = (LongColumnVector) rowBatch.cols[appPrd.getRf0Id()];
        LongColumnVector RF1 = (LongColumnVector) rowBatch.cols[appPrd.getRf1Id()];
        for (int row = 0; row < rowBatch.size; row++) {
            RF0.vector[row] = vrfArray[row << 1].toRF();
            RF1.vector[row] = vrfArray[(row << 1) + 1].toRF();
        }

        long eTime = System.currentTimeMillis();
        LOG.info("Build timeline index cost {} ms", eTime - sTime);
        // LOG.info("Assit struct {} bytes", SizeOf.deepSizeOf(counterArray) + SizeOf.deepSizeOf(cumulationArray));
    }
}
