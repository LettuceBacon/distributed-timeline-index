package xyz.mfj.dataManipulation.ConvertorImpl;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.mapred.OrcList;

public class TimestampConverter extends ConverterImpl {
    private final DateTimeFormatter dateTimeFormatter;
    
    public TimestampConverter(IntWritable offset, String nullString, String timestampFormat) {
        super(offset, nullString);
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(timestampFormat);
    }

    @Override
    public void convert(OrcList<Text> values, ColumnVector column, int row) {
        String currVal = values.get(offset).toString();
        if (currVal == null || nullString.equals(currVal)) {
            column.noNulls = false;
            column.isNull[row] = true;
        } else {
            TimestampColumnVector vector = (TimestampColumnVector) column;
            TemporalAccessor temporalAccessor =
                dateTimeFormatter.parseBest(currVal,
                    ZonedDateTime::from, OffsetDateTime::from, LocalDateTime::from);
            if (temporalAccessor instanceof ZonedDateTime) {
                ZonedDateTime zonedDateTime = ((ZonedDateTime) temporalAccessor);
                Timestamp timestamp = Timestamp.from(zonedDateTime.toInstant());
                vector.set(row, timestamp);
            } else if (temporalAccessor instanceof OffsetDateTime) {
                OffsetDateTime offsetDateTime = (OffsetDateTime) temporalAccessor;
                Timestamp timestamp = Timestamp.from(offsetDateTime.toInstant());
                vector.set(row, timestamp);
            } else if (temporalAccessor instanceof LocalDateTime) {
                Timestamp timestamp = Timestamp.valueOf((LocalDateTime) temporalAccessor);
                vector.set(row, timestamp);
            } else {
                column.noNulls = false;
                column.isNull[row] = true;
            }
        }
    }
}
