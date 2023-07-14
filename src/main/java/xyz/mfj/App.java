package xyz.mfj;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import xyz.mfj.dataProcess.DownloadYTTR2022;
import xyz.mfj.dataProcess.ProcessYTTR2022;

public class App 
{
    public static void main(String[] args) throws ParseException {
        // 定义选项
        Option displayHelper = new Option("h", "help", false, "Dispaly usage and help.");
        Option prepareData = Option.builder("p")
            .longOpt("prepare-data")
            .hasArg()
            .argName("dataSet")
            .desc("Prepare data set. Avaliable <dataSet>: \"yttr-2022\", \"lineitembih-<sf>\" where <sf> is the scale factor.")
            .build();
        Options options = new Options();
        options.addOption(prepareData);
        options.addOption(displayHelper);

        HelpFormatter helpFormatter = new HelpFormatter();
        String cmdSyntax = "[-h | --help] [ -p | --prepare-data [<dataset>] ] ";

        // 解析命令
        CommandLine commandLine = null;
        try {
            commandLine = new DefaultParser().parse(options, args);
        }
        catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            // 输出帮助
            helpFormatter.printHelp(cmdSyntax, options);
            throw exp;
        }

        // 执行命令
        boolean definedOptExists = false;

        // 准备实验数据
        String dsOptVal = commandLine.getOptionValue(prepareData);
        if (dsOptVal != null) {
            String[] dataSetDesc = dsOptVal.split("-");
            if (dataSetDesc[0].toLowerCase().equals("yttr")) {
                definedOptExists = true;
                DownloadYTTR2022.download();
                ProcessYTTR2022.process();
            }
            else if (dataSetDesc[0].toLowerCase().equals("lineitem")) {
                definedOptExists = true;
                System.out.println("Generating TPC-BiH is not supported in this version!");
            }
            else {
                System.err.printf("\"%s\" data set is not supported yet!", dsOptVal);
            }
        }

        
        
        // 如果全是没有定义过的选项或者单独输入help，输出帮助
        if (definedOptExists == false) {
            helpFormatter.printHelp(cmdSyntax, options);
        }

    }
}
