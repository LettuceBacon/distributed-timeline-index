package xyz.mfj.dataQuery.temporalGroupingQuery;

import java.util.function.Function;

import java.sql.Timestamp;
import java.time.LocalDate;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.orc.OrcConf;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcTimestamp;
import org.apache.orc.mapred.OrcValue;

import xyz.mfj.DtlConf;

public class WindowPartitioner extends Partitioner<OrcKey, OrcValue>
    implements Configurable
{
    private Configuration conf;
    private Function partitionFunc;

    @Override
    public int getPartition(OrcKey key, OrcValue value, int numPartitions) {
        // partId = (key - startTime) / windowLen
        return (int)partitionFunc.apply(key.key) % numPartitions;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        String overlapsTimeType = OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getString(conf);
        if (overlapsTimeType.equals("date")) {
            partitionFunc = new Function<DateWritable,Integer>() {
                long windowLen = conf.getLong(DtlConf.WINDOW_LENGTH, 0L);
                
                long rawStartTime = LocalDate.parse(
                    conf.get(DtlConf.OVERLAPS_START_TIME)
                ).toEpochDay();
                
                @Override
                public Integer apply(DateWritable key) {
                    return windowLen == 0 ? 0 :
                        (int)((key.getDays() - rawStartTime) / windowLen);
                }
                
            };
        }
        else {
            partitionFunc = new Function<OrcTimestamp,Integer>() {
                long windowLen = conf.getLong(DtlConf.WINDOW_LENGTH, 0L);
                
                long rawStartTime = Timestamp.valueOf(
                    conf.get(DtlConf.OVERLAPS_START_TIME)
                ).getTime();
                
                @Override
                public Integer apply(OrcTimestamp key) {
                    return windowLen == 0 ? 0 :
                        (int)((key.getTime() - rawStartTime) / windowLen);
                }
                
            };
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
    
}
