package xyz.mfj.dataProcess;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;

public class ProcessYTTR2022 {
    private static class Procedure implements Runnable {
        private FileStatus parquetFile;
        private Configuration conf;
        private String outputPath;
        private List<BiFunction> convertors;
        private Timestamp startBound;
        private Timestamp endBound;

        public Procedure(
            FileStatus parquetFile,
            Configuration conf,
            String outputPath,
            List<BiFunction> convertors,
            Timestamp startBound,
            Timestamp endBound
        ) {
            this.parquetFile = parquetFile;
            this.conf = conf;
            this.outputPath = outputPath;
            this.convertors = convertors;
            this.startBound = startBound;
            this.endBound = endBound;
        }

        
        @Override
        public void run() {
            try (
                ParquetFileReader reader = ParquetFileReader.open(
                    HadoopInputFile.fromPath(parquetFile.getPath(), conf)
                );
                ICSVWriter writer = new CSVWriterBuilder(new BufferedWriter(new FileWriter(outputPath)))
                    .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                    .build();
            ) {
                System.out.printf("Processing %s\n", parquetFile.getPath().getName());

                MessageType schema = reader.getFooter()
                    .getFileMetaData()
                    .getSchema();
                List<Type> fields = schema.getFields();
                PageReadStore pages = null;
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

                while ((pages = reader.readNextRowGroup()) != null) {
                    long rows = pages.getRowCount();
                    RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (int row = 0; row < rows; row++) {
                        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                        String[] rowVector = new String[fields.size()];

                        Timestamp start = (Timestamp)convertors.get(1).apply(simpleGroup, 1);
                        Timestamp end = (Timestamp)convertors.get(2).apply(simpleGroup, 2);

                        // 不符合条件的数据不写入csv
                        if (start.compareTo(endBound) >= 0 
                            || end.compareTo(startBound) <= 0
                            || start.compareTo(end) >= 0) 
                        {
                            continue;
                        }

                        for (int j = 0; j < fields.size(); j++) {
                            Object d = null;
                            try {
                                d = convertors.get(j).apply(simpleGroup, j);
                                rowVector[j] = d.toString();
                            } catch (RuntimeException e) {
                                // 如果parquet文件中有null值，会产生RuntimeException，输出的csv值为"null"
                                rowVector[j] = "null";
                            }
                        }
                        // 写入csv文件
                        writer.writeNext(rowVector);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.printf("Process finished: %s\n", parquetFile.getPath().getName());
        }
    }
        
    /**
     * 启动四线程将parquet格式的YTTR数据集转换为csv格式，同时完成数据清洗
     * 如果已存在，则覆盖写入
     */
    public static void process() {

        Configuration conf = new Configuration();
        String workDir = System.getProperty("user.dir") + "/YTTR";
        String outputDir = workDir + "/yellow_tripdata_2022";
        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }
        FileStatus[] parquetFiles = null;
        try {
            parquetFiles = FileSystem.get(conf).globStatus(
                new Path(workDir + "/*.parquet")
            );
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (parquetFiles.length != 12) {
            System.err.println("Data are less than twelve months, please make sure all twelve months' parquet file are in directory YTTR/");
            System.exit(0);
        }

        Timestamp startBound = Timestamp.valueOf("2022-01-01 00:00:00");
        Timestamp endBound = Timestamp.valueOf("2023-01-01 00:00:00");

        // 将数据从parquet读出并转为java类型的工具类
        BiFunction<SimpleGroup, Integer, Long> conv2Long = new BiFunction<SimpleGroup, Integer, Long>() {

            @Override
            public Long apply(SimpleGroup arg0, Integer arg1) {
                return arg0.getLong(arg1, 0);
            }

        };
        BiFunction<SimpleGroup, Integer, Double> conv2Double = new BiFunction<SimpleGroup, Integer, Double>() {

            @Override
            public Double apply(SimpleGroup arg0, Integer arg1) {
                return arg0.getDouble(arg1, 0);
            }

        };
        BiFunction<SimpleGroup, Integer, Timestamp> conv2Ts = new BiFunction<SimpleGroup, Integer, Timestamp>() {

            @Override
            public Timestamp apply(SimpleGroup arg0, Integer arg1) {
                return new Timestamp(arg0.getLong(arg1, 0) / 1000);
            }

        };
        BiFunction<SimpleGroup, Integer,String> conv2Str = new BiFunction<SimpleGroup, Integer, String>() {

            @Override
            public String apply(SimpleGroup arg0, Integer arg1) {
                return arg0.getString(arg1, 0);
            }

        };
        List<BiFunction> convertors = new ArrayList<>();
        convertors.add(conv2Long);
        convertors.add(conv2Ts);
        convertors.add(conv2Ts);
        convertors.add(conv2Double);
        convertors.add(conv2Double);
        convertors.add(conv2Double);
        convertors.add(conv2Str);
        convertors.add(conv2Long);
        convertors.add(conv2Long);
        convertors.add(conv2Long);
        convertors.add(conv2Double);
        convertors.add(conv2Double);
        convertors.add(conv2Double);
        convertors.add(conv2Double);
        convertors.add(conv2Double);
        convertors.add(conv2Double);
        convertors.add(conv2Double);
        convertors.add(conv2Double);
        convertors.add(conv2Double);

        ExecutorService executors = Executors.newFixedThreadPool(4);
        for (int i = 0; i < parquetFiles.length; i++) {
            String outputPath = String.format("%s/yellow_tripdata_2022/yellow_tripdata_2022-%02d.csv", workDir, i + 1);
            File outputFile = new File(outputPath);
            FileStatus parquetFile = parquetFiles[i];
            
            executors.submit(new Procedure(parquetFile, conf, outputPath, convertors, startBound, endBound));
        }
        executors.shutdown();
        try {
            if (!executors.awaitTermination(5, TimeUnit.MINUTES)) {
                System.err.println("Time out!");
                executors.shutdownNow();
            }
        } catch (Exception e) {
            executors.shutdownNow();
            e.printStackTrace();
        }
    }
}


