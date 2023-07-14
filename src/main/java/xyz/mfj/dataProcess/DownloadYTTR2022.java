package xyz.mfj.dataProcess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

public class DownloadYTTR2022 {
    /**
     * 启动四线程从指定位置下载数据集到`%CLASSPATH/YTTR`
     * 如果已存在同名文件，则覆盖写入
     */
    public static void download() {
        String workDir = System.getProperty("user.dir") + "/YTTR";
        String downloadURLsFile = workDir + "/downloadURLs";
        String outputFormat = workDir + "/yellow_tripdata_2022-%02d.parquet";
        
        List<URL> downloadURLs = new ArrayList<>();
        try (
            BufferedReader br = new BufferedReader(new FileReader(downloadURLsFile));
        ) {
            String urlStr = null;
            while ((urlStr = br.readLine()) != null) {
                downloadURLs.add(new URL(urlStr));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (int i = 0; i < downloadURLs.size(); i++) {
            URL downloadUrl = downloadURLs.get(i);
            String outputPath = String.format(outputFormat, i+1);
            File outputFile = new File(outputPath);
            executor.submit(
                () -> {
                    System.out.printf("Downloading from %s into %s\n",downloadUrl, outputFile);

                    // 使用Apache Commons IO实现下载文件
                    try {
                        FileUtils.copyURLToFile(downloadUrl, outputFile);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
        
                    // // 使用Java原生API实现下载文件
                    // try (
                    //     ReadableByteChannel rbc = Channels.newChannel(downloadUrl.openStream());
                    //     FileOutputStream fos = new FileOutputStream(outputPath);

                    // ){
                    //     fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
                    //     fos.flush();
                    // } catch (Exception e) {
                    //     e.printStackTrace();
                    // }

                    System.out.printf("Download finished: %s\n", outputFile);
                }
            );

        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                executor.shutdownNow();
                System.err.println("Time out!");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            e.printStackTrace();
        }
        
    }
}
