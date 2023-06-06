package xyz.mfj.readers;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.NoDynamicValuesException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Operator;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.SchemaEvolution;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StripeReaderDebug {
    private static final Logger LOG = LoggerFactory.getLogger(StripeReaderDebug.class);
    
    @Test
    public void pickStripesDebug() throws IllegalArgumentException, IOException {
        Configuration conf = new Configuration();
        OrcConf.COMPRESS.setString(conf, "NONE");
        OrcConf.STRIPE_SIZE.setLong(conf, 67108864); 
        OrcConf.STRIPE_ROW_COUNT.setLong(conf, 1095000);
        OrcConf.ROWS_BETWEEN_CHECKS.setInt(conf, 1095000);
        
        String filePath = "lineitembih.orc";
        EnhancedOrcReader fileReader = new EnhancedOrcReader(new Path(filePath), OrcFile.readerOptions(conf));
        
        SearchArgument sa = SearchArgumentFactory.newBuilder()
            // .lessThan("partKey", PredicateLeaf.Type.LONG, Long.valueOf(10000L))
            .between("orderKey", PredicateLeaf.Type.LONG, Long.valueOf(2L), Long.valueOf(3L))
            // .lessThan("extendedPrice", PredicateLeaf.Type.DECIMAL, new HiveDecimalWritable("10000.25"))
            // .equals("returnFlag", PredicateLeaf.Type.STRING, "A  ")
            // .lessThanEquals("shipDate", PredicateLeaf.Type.DATE, Date.valueOf("1992-02-03"))
            .build();
        // boolean[] colsIncluded = new boolean[21];
        // colsIncluded[0] = true; // 0是顶层struct
        // colsIncluded[1] = true;
        // colsIncluded[2] = true;
        // colsIncluded[3] = true;
        // colsIncluded[17] = true;
        // colsIncluded[18] = true;
        Reader.Options options = fileReader.options()
            // .include(colsIncluded)
            .allowSARGToFilter(true)
            .searchArgument(sa, new String[0]);
        
        StripeReader stripeReader = new StripeReaderImpl(fileReader, options);
        VectorizedRowBatch rowBatch = fileReader.getSchema().createRowBatch(5000);
        while (stripeReader.nextBatch(rowBatch)) {
            System.out.println(rowBatch.size);
        }
        
        
        stripeReader.close();
        
    }
    
    
}
