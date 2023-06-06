package xyz.mfj.dataManipulation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVParserBuilder;
import com.opencsv.ICSVParser;

// 将TextInputFormat传来的每个KV用csvParser解析成一组字符串
public class CsvInputMapper extends Mapper<LongWritable, Text, IntWritable, OrcValue>{
    private static final Logger LOG = LoggerFactory.getLogger(CsvInputMapper.class);
    private static final String SEPARATORCHAR = ",";
    private static final String QUOTECHAR = "\"";
    private static final String ESCAPECHAR = "\\";
    public static final String SEPARATORCHAR_CONF_NAME = "separator-char";
    public static final String QUOTECHAR_CONF_NAME = "quote-char";
    public static final String ESCAPECHAR_CONF_NAME = "escape-char";
    public static final String HASHEADERLINE_CONF_NAME = "has-header-line";
        
    private int numReduceTasks;
    private ICSVParser csvParser;
    private OrcList<Text> result;
    private boolean hasHeaderLine;
    private int mapExeCnt;
    private IntWritable reduceId;
    private OrcValue valueOut;
    private long sTime;
    private long eTime;
    
    @Override
    protected void setup(Mapper<LongWritable, Text, IntWritable, OrcValue>.Context context)
            throws IOException, InterruptedException 
    {
        numReduceTasks = context.getNumReduceTasks();
        Configuration jobConf = context.getConfiguration();
        char separatorChar = jobConf.get(SEPARATORCHAR_CONF_NAME, SEPARATORCHAR).toCharArray()[0];
        char quoteChar = jobConf.get(QUOTECHAR_CONF_NAME, QUOTECHAR).toCharArray()[0];
        char escapeChar = jobConf.get(ESCAPECHAR_CONF_NAME, ESCAPECHAR).toCharArray()[0];
        hasHeaderLine = jobConf.getBoolean(HASHEADERLINE_CONF_NAME, false);
        csvParser = new CSVParserBuilder().withSeparator(separatorChar)
            .withQuoteChar(quoteChar)
            .withEscapeChar(escapeChar)
            .build();
        result = new OrcList<Text>(TypeDescription.createList(TypeDescription.createString()));
        mapExeCnt = 0;
        reduceId = new IntWritable();
        valueOut = new OrcValue();

        sTime = System.currentTimeMillis();
    }
    
    @Override
    protected void map(LongWritable key, Text value,
            Mapper<LongWritable, Text, IntWritable, OrcValue>.Context context)
            throws IOException, InterruptedException 
    {
        if (hasHeaderLine && key.compareTo(new LongWritable(0L)) == 0) {
            return;
        }
        
        String[] line = csvParser.parseLine(value.toString());
        
        for (int i = 0; i < line.length; i++) {
            result.add(new Text(line[i]));
        }
        reduceId.set(mapExeCnt % numReduceTasks);
        valueOut.value = result;
        context.write(reduceId, valueOut);
        
        mapExeCnt++;
        result.clear();
        
    }
    
    @Override
    protected void cleanup(Mapper<LongWritable, Text, IntWritable, OrcValue>.Context context)
            throws IOException, InterruptedException {
        eTime = System.currentTimeMillis();
        LOG.info("Mapper {} cost {} ms, read {} row(s)", 
            context.getTaskAttemptID().getTaskID().getId(), 
            eTime - sTime,
            mapExeCnt);
    }
}
