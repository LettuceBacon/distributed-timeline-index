package xyz.mfj.dataDefiniation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import net.sourceforge.sizeof.SizeOf;

/**
 * 中缀计数排序
 */

// 主要思想：
// 将一个待排序数据转为long型，将long型的bit位分为三部分，分别是位于高位的最长公共前缀，位于中间的计数键和位于低位的后缀。
// 最长公共前缀不影响数据顺序，去掉它。
// 稀疏的数据不仅会浪费计数数组的空间，还会浪费遍历计数数组的时间，因此通过去掉后缀来保证数据的密集。假设数据是均匀分布的，那么去掉后缀后计数桶的个数少于数据总量时，一个计数桶中至少有一个计数，可以认为此时不存在数据稀疏问题。
// 数据分区能够保证数据总量较小，保证计数数组内存占用上限较小。
// 计数键中，高位部分越稀疏，整体数据在计数桶中分布越稀疏，因此从高位开始计算固定位数的计数键的稀疏度，直到一个非常稀疏（先验知识设置一个稀疏阈值）bit区间出现，将这个区间内的bit以及后续bit都视为后缀。
// 用待排数据中最大值和最小值计数键之差作为计数桶的个数，建立连续的计数桶，然后根据所有数据的计数键进行计数排序。
// 这样排序后所有数据就基本有序了，再使用归并排序使所有数据最终有序。
// 如果出现数据倾斜问题，对倾斜的数据调用该算法。目前采用二八定律判定是否存在数据倾斜，即如果20%的计数桶计数的和超过了总数据量的80%，就认为存在严重的数据倾斜问题。

// 时间复杂度分析：
// 假设有n个数待排序，计数桶的个数接近数据规模，为n
// 获取最长公共前缀和最大最小值需要`O(n)`；读取数据，然后按数据计数键计数需要`O(n)`；读取计数数组，构造累积计数数组需要`O(n)`；读取数据，然后根据累积计数数组将数据放到数组对应位置需要`O(n)`
// 最佳情况，计数排序后数据即完全有序，则只需要一轮归并排序`O(n)`，最终时间复杂度为`O(n)`
// 一般情况，计数排序后数据不完全有序，需要执行若干轮归并排序，归并排序时间复杂度为`O(nlog(n))`，最终时间复杂度为`O(n + nlog(n)`，由于计数排序后数据基本有序，因此归并排序的实际执行时间相比于完全乱序的情况要小很多。

// 空间复杂度分析：
// 最多有n个计数桶，空间复杂度为`O(n)`，归并排序需要`O(n)`，因此最终空间复杂度是`O(n)`

// 不足：
// 使用二八定律判断数据倾斜不能判断有多个数据聚类的问题，也不是一个性能最优的判断方式，是使用先验经验的判断方式。
// 后缀位数的计算是以数据是均匀分布为前提的，而大多数数据都不是均匀分布的，因此不能完全解决数据稀疏问题。

public class InfixCountingSort {
    private static final Logger LOG = LoggerFactory.getLogger(InfixCountingSort.class);
    // 对一个本身可以排序的，可以将排序用的键用long数值表示的对象数组进行排序
    
    private static final int MAX_SORT_KEY_BIT_NUM = Integer.SIZE - 1;
    // private static final int FIXED_STRIDE = 8;
    
    public static void sort(Comparable[] values, Function<Comparable, Long> digitalKeyOf) {
        long minDigitKey = Long.MAX_VALUE;
        long maxDigitKey = 0L;
        // 最长共同前缀位数 Longest-Common-Prefix
        int lcpBitNum = Long.SIZE;
        // 任意一个待排序的数据的数值键
        long digitKey = digitalKeyOf.apply(values[0]);
        // 所有可能的最长公共前缀数组
        long[] lcpArray = new long[Long.SIZE];
        // 不包括符号位的前缀i+1个bit为1的数字的数组
        long[] prefixMaskArray = new long[Long.SIZE];
        
        for (int i = 0; i < lcpBitNum; ++i) {
            prefixMaskArray[i] = Long.MAX_VALUE >> (Long.SIZE - i - 1) << (Long.SIZE - i - 1);
            lcpArray[i] = prefixMaskArray[i] & digitKey;
        }
        
        // 拿到values，读一遍拿到最大值、最小值和最长前缀，以及填写fixedStrideCounterArray
        for (int i = 0; i < values.length; ++i) {
            long temp = digitalKeyOf.apply(values[i]);
            if (minDigitKey > temp) minDigitKey = temp;
            if (maxDigitKey < temp) maxDigitKey = temp;
            for (int j = 0; j < lcpBitNum; ++j) {
                if (lcpArray[j] != (temp & prefixMaskArray[j])) {
                    lcpBitNum = j;
                    break;
                }
            }
        }
        lcpBitNum += 1;
        
        
        // 先验经验获取后缀位数
        // lineitembih
        // int suffixBitNum = 0;
        // YTTR
        // int suffixBitNum = 16;
        // long minPartitionKey = minDigitKey >> suffixBitNum;
        // long maxPartitionKey = maxDigitKey >> suffixBitNum;
        
        // 自动获取后缀位数
        int suffixBitNum = 0;
        long minPartitionKey = minDigitKey >> suffixBitNum;
        long maxPartitionKey = maxDigitKey >> suffixBitNum;
        while ((maxPartitionKey - minPartitionKey + 1L) >= (long)values.length) {
            suffixBitNum++;
            minPartitionKey = minDigitKey >> suffixBitNum;
            maxPartitionKey = maxDigitKey >> suffixBitNum;
        }
        
        int counterArraySize = (int)(maxPartitionKey - minPartitionKey + 1L);
        int[] counterArray = new int[counterArraySize];

        for (int i = 0; i < values.length; ++i) {
            long temp = digitalKeyOf.apply(values[i]);
            int counterId = (int)((temp >> suffixBitNum) - minPartitionKey);
            counterArray[counterId]++;
        }
        
        int[] cumulativeCountArray = new int[counterArraySize];
        // 20%的计数桶
        int skewThresholdCounterNum = (int)(counterArraySize * 0.2);
        // 80%的数据量
        int skewThresholdCountNum = (int)(values.length * 0.8);
        int countSum = counterArray[0];
        int skewStartIdx = -1;
        int skewLength = -1;
        for (int i = 1; i < counterArraySize; ++i) {
            cumulativeCountArray[i] = cumulativeCountArray[i - 1] + counterArray[i - 1];
            if (i < skewThresholdCounterNum) {
                countSum += counterArray[i];
            }
            else {
                if (countSum > skewThresholdCountNum) {
                    skewStartIdx = cumulativeCountArray[i - skewThresholdCounterNum];
                    skewLength = countSum;
                }
                countSum = countSum + counterArray[i] - counterArray[i - skewThresholdCounterNum];
            }
        }
        
        
        Comparable[] result = new Comparable[values.length];
        for (int i = 0; i < values.length; ++i) {
            long temp = digitalKeyOf.apply(values[i]);
            int counterId = (int)((temp >> suffixBitNum) - minPartitionKey);
            result[cumulativeCountArray[counterId]++] = values[i];
        }
        
        // 对倾斜部分的数据再执行本算法
        Comparable[] skewData = null;
        if (skewLength > skewThresholdCountNum) {
            skewData = new Comparable[skewLength];
            System.arraycopy(result, skewStartIdx, skewData, 0, skewLength);
            sort(skewData, digitalKeyOf);
            System.arraycopy(skewData, 0, result, skewStartIdx, skewLength);
        }
        
        // 调用归并排序，使得计数桶内的数字有序
        Arrays.sort(result);
        
        System.arraycopy(result, 0, values, 0, result.length);
        
        // LOG.info("Assist struct {} bytes", 
        //     SizeOf.deepSizeOf(counterArray) 
        //     + SizeOf.deepSizeOf(cumulativeCountArray) 
        //     + SizeOf.sizeOf(result)
        //     + SizeOf.sizeOf(skewData)
        // );
    }
    
    
    // /**
    //  * 第二版前缀分区排序
    //  */

    // // 主要思想：
    // // 将一个待排序数据转为long型，将long型的bit位分为三部分，分别是位于高位的最长公共前缀，位于中间的分区键和位于低位的排序键，排除最长公共前缀的影响。
    // // 用待排数据中最大值和最小值的分区键之差作为分区数，建立连续的若干分区（类似roaringbitmap）。
    // // 利用数据的分区键将long型的数据散列到相应分区内。
    // // 然后在分区内对所有数据进行二次排序，数据较密集的分区（即数据量比排序键所能表示的最大数还大时）使用计数排序，否则使用红黑树计数排序，并构造累积计数
    // // 最后根据数据的分区键和排序键散列找到对应的累积计数，将数据放置在累积计数对应的顺序位置上，对于timestamp类型还需要进行插入排序让其在纳秒级别上有序
    // // 分区共有三种，空分区，没有数据，数组计数排序分区和红黑树计数排序分区

    // // 时间复杂度分析：
    // // 假设有n个数待排序，
    // // 获取n个数的分区键并建立分区需要`O(n)`，得到分区键为m位，即最多有`2^m`个分区
    // // 最佳情况，除了空分区，每个分区都使用数组计数排序，每个分区平均有`\frac{n}{2^m}`个待排序数，分区内排序时间为`O(\frac{n}{2^m})`，构造累积计数的时间为`O(\frac{n}{2^m})`，根据累积计数对数据排序的时间为`O(n)`，整体排序时间为`O(2n + 2* 2^m * \frac{n}{2^m})`，约为`O(n)`，接近一般计数排序
    // // 最差情况，只有少量分区使用计数排序，大多数分区使用红黑树计数排序，每个分区平均有`\frac{n}{2^m}`个待排序数，分区内排序时间为`O(\frac{n}{2^m} * log(\frac{n}{2^m}))`，构造累积计数时间为`O(\frac{n}{2^m} * log(\frac{n}{2^m}))`，根据累积计数对数据排序的时间为`O(n * log(\frac{n}{2^m}))`，整体排序时间为`O(n + 2 * 2^m * (\frac{n}{2^m} * log(\frac{n}{2^m})) + n * log(\frac{n}{2^m}))`，约为`O(n * log(\frac{n}{2^m}))`，比红黑树B数略快

    // // 空间复杂度分析：
    // // 最多有`2^m`个分区
    // // 最佳情况，几乎没有空分区，且分区内数据密度非常大使得分区内只有常数级数量的计数桶，占用空间`O(2^m)`
    // // 最差情况，每个分区都恰好装略多于`\frac{n}{2^m}`个待排序数，占用空间为`O(n + 2^m)`

    // // 不足：
    // // 不能很好地解决数据稀疏问题，数据稀疏时（最差情况）仅比红黑树B树等略快且分区空间占用很高
    
    // // Build timeline index cost 322 ms
    // // Build timeline index cost 323 ms
    // // Build timeline index cost 203 ms
    // // Assist struct 77304 bytes
    
    // private static final int MAX_PARTITION_KEY_BIT_NUM = Integer.SIZE - 1;
    // private static final int MAX_SORT_KEY_BIT_NUM = Integer.SIZE;
    
    // public static void sort(Comparable[] values, Function<Comparable, Long> digitalKeyOf) {
    //     long minDigitKey = Long.MAX_VALUE;
    //     long maxDigitKey = 0L;
    //     // 最长共同前缀位数 Longest-Common-Prefix
    //     int lcpBitNum = Long.SIZE;
    //     // 任意一个待排序的数据
    //     long digitKey = digitalKeyOf.apply(values[0]);
    //     // 所有可能的最长公共前缀数组
    //     long[] lcpArray = new long[Long.SIZE];
    //     // 不包括符号位的前缀i+1个bit为1的数字的数组
    //     long[] prefixMaskArray = new long[Long.SIZE];
        
    //     for (int i = 0; i < lcpBitNum; ++i) {
    //         prefixMaskArray[i] = Long.MAX_VALUE >> (Long.SIZE - i - 1) << (Long.SIZE - i - 1);
    //         lcpArray[i] = prefixMaskArray[i] & digitKey;
    //     }
        
    //     // 拿到values，读一遍拿到最大值、最小值和最长前缀
    //     for (int i = 0; i < values.length; ++i) {
    //         long temp = digitalKeyOf.apply(values[i]);
    //         if (minDigitKey > temp) minDigitKey = temp;
    //         if (maxDigitKey < temp) maxDigitKey = temp;
    //         for (int j = 0; j < lcpBitNum; ++j) {
    //             if (lcpArray[j] != (temp & prefixMaskArray[j])) {
    //                 lcpBitNum = j;
    //                 break;
    //             }
    //         }
    //     }
    //     lcpBitNum += 1;
        
    //     // 排除最长前缀，让分区键位数满足条件
    //     // 条件是，需要让平均每个分区分到的数据量大于分区内计数桶的数量
    //     int sortKeyBitNum = MAX_SORT_KEY_BIT_NUM;
    //     long minPartitionKey = minDigitKey;
    //     long maxPartitionKey = maxDigitKey;
    //     int counterThreshold = 0;
    //     do {
    //         sortKeyBitNum--;
    //         minPartitionKey = minDigitKey >> sortKeyBitNum;
    //         maxPartitionKey = maxDigitKey >> sortKeyBitNum;
    //     }
    //     while (sortKeyBitNum > 0
    //         && ((maxPartitionKey - minPartitionKey) <= 0
    //         || (long)values.length <= (maxPartitionKey - minPartitionKey + 1L) * (1L << sortKeyBitNum))
    //         && Long.SIZE - lcpBitNum - sortKeyBitNum < MAX_PARTITION_KEY_BIT_NUM
    //     );
    //     counterThreshold = 1 << sortKeyBitNum;
    //     if (values.length * 10 < (maxPartitionKey - minPartitionKey + 1L)) {
    //         Arrays.sort(values);
    //         return;
    //     }
        
    //     // 构造分区，先用vector存，再读一遍values，先分区而不排序
    //     Object[] partitions = new Object[(int)(maxPartitionKey - minPartitionKey + 1L)];
    //     int[] sizes = new int[(int)(maxPartitionKey - minPartitionKey + 1L)];
    //     // Comparable[] sortedArray;
    //     // TreeMap<Comparable, Integer> redBlackTree;
    //     Vector<Long> longDump;
    //     for (int i = 0; i < values.length; ++i) {
    //         long temp = digitalKeyOf.apply(values[i]);
    //         int partitionId = (int)((temp >> sortKeyBitNum) - minPartitionKey);
    //         if (partitions[partitionId] == null) {
    //             partitions[partitionId] = new Vector<>(counterThreshold, counterThreshold);
    //         }
    //         longDump = (Vector<Long>) partitions[partitionId];
    //         longDump.add(temp);
    //         sizes[partitionId]++;
    //     }

    //     // 后缀i+1个bit为1的数字的数组
    //     long suffixMask = Long.MAX_VALUE >> (Long.SIZE - 1 - sortKeyBitNum);
    //     int counterArraySize = 1 << sortKeyBitNum;
        
    //     // 读一遍分区，根据分区内数量和界限采用计数排序或者红黑树排序
    //     TreeMap<Long, Integer> counterRBT;
    //     int[] counterArray;
    //     for (int i = 0; i < partitions.length; ++i) {
    //         if (partitions[i] == null) {
    //             continue;
    //         }
    //         longDump = (Vector<Long>)partitions[i];
    //         if (sizes[i] > counterThreshold) {
    //             // 转数组计数排序
    //             partitions[i] = counterArray = new int[counterArraySize];
    //             for (long l : longDump) {
    //                 counterArray[(int)(l & suffixMask)]++;
    //             }
    //         }
    //         else {
    //             // 转红黑树计数排序
    //             partitions[i] = counterRBT = new TreeMap<>();
    //             for (long l : longDump) {
    //                 Integer count = counterRBT.get(l);
    //                 if (count == null) {
    //                     counterRBT.put(l, 1);
    //                 }
    //                 else {
    //                     counterRBT.put(l, count + 1);
    //                 }
    //             }
    //         }
    //     }
        
    //     // 再读一遍分区，构造累积计数排序
    //     int preserveCount = 0;
    //     int lastCount = 0;
    //     int lastCumulativeCount = 0;
    //     for (int i = 0; i < partitions.length; ++i) {
    //         if (partitions[i] == null) {
    //             continue;
    //         }
    //         if (sizes[i] > counterThreshold) {
    //             counterArray = (int[])partitions[i];
    //             for (int j = 0; j < counterArray.length; ++j) {
    //                 preserveCount = counterArray[j];
    //                 counterArray[j] = lastCount + lastCumulativeCount;
    //                 lastCount = preserveCount;
    //                 lastCumulativeCount = counterArray[j];
    //             }
    //         }
    //         else {
    //             counterRBT = (TreeMap<Long, Integer>) partitions[i];
    //             for (long l : counterRBT.navigableKeySet()) {
    //                 preserveCount = counterRBT.get(l);
    //                 counterRBT.put(l, lastCount + lastCumulativeCount);
    //                 lastCumulativeCount = lastCount + lastCumulativeCount;
    //                 lastCount = preserveCount;
    //             }
    //         }
    //     }
        
    //     // 再读一遍values，根据values的key找到指定的位置，然后放到values里
    //     Comparable[] result = new Comparable[values.length];
    //     for (int i = 0; i < values.length; ++i) {
    //         long temp = digitalKeyOf.apply(values[i]);
    //         int partitionId = (int)((temp >> sortKeyBitNum) - minPartitionKey);
    //         if (sizes[partitionId] > counterThreshold) {
    //             counterArray = (int[])partitions[partitionId];
    //             result[counterArray[(int)(temp & suffixMask)]++] = values[i];
    //         }
    //         else {
    //             counterRBT = (TreeMap<Long, Integer>) partitions[partitionId];
    //             int cumulativeCount = counterRBT.get(temp);
    //             int idx = cumulativeCount;
    //             while (idx > 0
    //                 && result[idx - 1] != null 
    //                 && result[idx - 1].compareTo(values[i]) > 0
    //             ) {
    //                 result[idx] = result[idx - 1];
    //                 idx--;
    //             }
    //             result[idx] = values[i];
    //             counterRBT.put(temp, cumulativeCount + 1);
    //         }
    //     }
    //     // Arrays.sort(result);
    //     System.arraycopy(result, 0, values, 0, result.length);
        
    //     // LOG.info("Assist struct {} bytes", 
    //     //     SizeOf.deepSizeOf(partitions) + SizeOf.deepSizeOf(sizes));
    // }
    
    
    /*
     * 第一版前缀分区排序
     */
    
        
    // 主要思想：
    // 将一个待排序对象转为其数值形式，一般为long型，将long型的bit位分为三部分，分别是位于高位的最长公共前缀，
    // 位于中间的分区键和位于低位的排序键，排除最长公共前缀的影响，根据分区键建立对应数量的连续的分区（类似计数
    // 排序），利用分区键将所有数据映射到相应分区内，然后在分区内对所有数据进行二次排序，数据量低于一定数量时用插入排序，大于时用红黑树排序。

    // 算法：
    // 将version从rowbatch中读出并转为long型，同时记录其最长共同前缀位数p和最大最小的version
    // 设定一个分区键位数m（最小为0），使得排序键为5bit，从最小值和最大值中获取最小和最大的分区键
    // 建立（最大分区键-最小分区键）个分区，初始化每个分区内使用长度为46的数组存储待排vrf
    // 再次读取所有version，根据分区键将version对应的vrf分到不同分区里排序并存储，如果分区装载超过3个就构造一个红黑树装载vrf
    // 最后遍历所有分区取出排序后的数据

    // 时间复杂度分析：
    // 假设有n个数待排序，分区键为m位
    // 获取n个数的分区键并分区需要`O_{n}`
    // 最多有`2^m`个分区
    // 最佳情况，每个分区都只装载3个以内的待排序数，分区内排序时间为`O_{1}`，整体排序时间为`O_{n+2^m}`
    // 一般情况，每个分区平均有`\frac{n}{2^m}`个待排序数，分区内排序时间为`O_{\frac{n}{2^m} log(\frac{n}{2^m})}`，整体排序时间为`O_{\frac{n}{2^m} * log(\frac{n}{2^m}) * 2^m + n}`
    // 最差情况，有一个分区装载了几乎所有待排序数，这个分区的排序时间为`O_{n log(n)}`，整体排序时间为`O_{n * log(n) + n}

    // 空间复杂度分析：
    // 最多有`2^m`个分区
    // 最佳情况，是时间复杂度最长情况，`O_{n + 2^m}`
    // 最差情况，是时间复杂度最佳情况，`O_{3 * 2^m}`

    // 不足：
    // 目前只能利用先验知识设置分区键位数，无法保证大部分分区内装载量在47个以内
    // 无法解决数据稀疏问题
    // 稀疏问题是指在最大最小分区键之间的大部分分区没有数据
    // 倾斜问题是数据的分区键大多数相同，少数不同

    // private static final int MAX_PARTITION_KEY_BIT_NUM = Integer.SIZE - 1;
    // private static final int MAX_SORT_KEY_BIT_NUM = Integer.SIZE;
    // private static final int INSERTION_THRESHOLD = 47;
    // // private static final int INSERTION_THRESHOLD = 180; // 估计是多数分区内数据超过界限导致性能趋近于红黑树
    
    
    // //  Build timeline index cost 700 ms
    // //  Build timeline index cost 698 ms
    // //  Build timeline index cost 613 ms
    // // Assist struct 105846232 bytes
    
    // public static void sort(Comparable[] values, Function<Comparable, Long> digitalKeyOf) {
    //     long minDigitKey = Long.MAX_VALUE;
    //     long maxDigitKey = 0L;
    //     // 最长共同前缀位数 Longest-Common-Prefix
    //     int lcpBitNum = Long.SIZE;
    //     // 任意一个待排序的数据
    //     long digitKey = digitalKeyOf.apply(values[0]);
    //     // 所有可能的最长公共前缀数组
    //     long[] lcpArray = new long[Long.SIZE];
    //     // 前缀i+1个bit为1的数字的数组
    //     long[] prefixArray = new long[Long.SIZE];
        
    //     for (int i = 0; i < lcpBitNum; ++i) {
    //         prefixArray[i] = Long.MAX_VALUE >> (Long.SIZE - i - 1) << (Long.SIZE - i - 1);
    //         lcpArray[i] = prefixArray[i] & digitKey;
    //     }
        
    //     for (int i = 0; i < values.length; ++i) {
    //         long temp = digitalKeyOf.apply(values[i]);
    //         if (minDigitKey > temp) minDigitKey = temp;
    //         if (maxDigitKey < temp) maxDigitKey = temp;
    //         for (int j = 0; j < lcpBitNum; ++j) {
    //             if (lcpArray[j] != (temp & prefixArray[j])) {
    //                 lcpBitNum = j;
    //                 break;
    //             }
    //         }
    //     }
    //     lcpBitNum += 1;
        
    //     int sortKeyBitNum = MAX_SORT_KEY_BIT_NUM;
    //     long minPartitionKey = minDigitKey >> sortKeyBitNum;
    //     long maxPartitionKey = maxDigitKey >> sortKeyBitNum;
    //     while (sortKeyBitNum > 0
    //         && values.length / (int)(maxPartitionKey - minPartitionKey + 1L) > INSERTION_THRESHOLD
    //         && Long.SIZE - lcpBitNum - sortKeyBitNum < MAX_PARTITION_KEY_BIT_NUM
    //     ) {
    //         sortKeyBitNum--;
    //         minPartitionKey = minDigitKey >> sortKeyBitNum;
    //         maxPartitionKey = maxDigitKey >> sortKeyBitNum;
    //     }
        
    //     Object[] partitions = new Object[(int)(maxPartitionKey - minPartitionKey + 1L)];
    //     int[] sizes = new int[(int)(maxPartitionKey - minPartitionKey + 1L)];
    //     Comparable[] sortedArray;
    //     TreeMap<Comparable, Integer> redBlackTree;
        
    //     for (int i = 0; i < values.length; ++i) {
    //         int partitionId = (int)((digitalKeyOf.apply(values[i]) >> sortKeyBitNum) - minPartitionKey);
            
    //         if (sizes[partitionId] == 0) {
    //             partitions[partitionId] = sortedArray = new Comparable[INSERTION_THRESHOLD];
    //             sortedArray[0] = values[i];
    //         }
    //         else if (sizes[partitionId] < INSERTION_THRESHOLD) {
    //             sortedArray = (Comparable[])partitions[partitionId];
    //             insertionSort(sortedArray, sizes[partitionId], values[i]);
    //         }
    //         else if (sizes[(int)partitionId] == INSERTION_THRESHOLD) {
    //             sortedArray = (Comparable[])partitions[partitionId];
    //             partitions[(int)partitionId] = redBlackTree = new TreeMap<>();
    //             for (Comparable val : sortedArray) {
    //                 redBlackTree.put(val, null);
    //             }
    //             redBlackTree.put(values[i], null);
    //         }
    //         else { // sizes[partitionId] > insertionThreshold
    //             redBlackTree = (TreeMap<Comparable, Integer>)partitions[(int)partitionId];
    //             redBlackTree.put(values[i], null);
    //         }
    //         sizes[partitionId]++;
    //     }
        
    //     // 遍历读取
    //     // 从第一个分区开始，找到每一个没有读取的分区
    //     // 如果大小小于等于47，转为数组并遍历，取出的每个数填到values里，
    //     // 如果大小大于47，从treemap获取iter，取出的每个数填到values里
    //     int valuesId = 0;
    //     for (int i = 0; i < partitions.length; ++i) {
    //         if (partitions[i] == null) {
    //             continue;
    //         }
    //         if (sizes[i] <= INSERTION_THRESHOLD) {
    //             sortedArray = (Comparable[]) partitions[i];
    //             for (int readedSize = 0; readedSize < sizes[i]; ++readedSize) {
    //                 values[valuesId++] = sortedArray[readedSize];
    //             }
    //         }
    //         else {
    //             redBlackTree = (TreeMap<Comparable, Integer>) partitions[i];
    //             Iterator<Comparable> iter = redBlackTree.navigableKeySet().iterator();
    //             while (iter.hasNext()) {
    //                 values[valuesId++] = iter.next();
    //             }
    //         }
    //     }
        
    //     // LOG.info("Assist struct {} bytes", 
    //     //     SizeOf.deepSizeOf(partitions) + SizeOf.deepSizeOf(sizes));
    // }
    
    // private static void insertionSort(Comparable[] sortedArray, int size, Comparable elem) {
    //     int i = size - 1;
    //     while (i >= 0 && sortedArray[i].compareTo(elem) > 0) {
    //         sortedArray[i + 1] = sortedArray[i];
    //         --i;
    //     }
    //     sortedArray[i + 1] = elem;
    // }
    
}
