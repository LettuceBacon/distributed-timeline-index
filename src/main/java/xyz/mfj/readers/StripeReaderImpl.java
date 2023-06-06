package xyz.mfj.readers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.NoDynamicValuesException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Operator;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DataReader;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.filter.BatchFilter;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.orc.impl.TreeReaderFactory;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.StripePlanner;
import org.apache.orc.impl.reader.tree.BatchReader;
import org.apache.orc.impl.reader.tree.TypeReader;
import org.apache.orc.impl.filter.FilterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StripeReaderImpl implements StripeReader{
    private static final Logger LOG = LoggerFactory.getLogger(StripeReaderImpl.class);
    private static final boolean isLogDebugEnabled = LOG.isDebugEnabled();
    protected final Path path;
    private final List<StripeInformation> stripes;
    // private long totalRowCount;
    // the file included columns indexed by the file's column ids.
    // 包括顶层struct在内
    private final boolean[] colsIncluded;
    // // 当前条带已经读到rowBatch里的行和多余的行的数量
    // private long readedRowSize = 0;
    // 表示下一个要从文件读到reader中的stripe的id
    private int stripeIdx;
    // 当前条带行数
    private long rowCountInStripe = 0;
    private final BatchReader reader;
    private final DataReader dataReader;
    private final int maxDiskRangeChunkLimit;
    private final StripePlanner planner;
    // identifies the type of read, ALL(read everything), LEADERS(read only the filter columns)
    private final TypeReader.ReadPhase startReadPhase;
    // 表示reader里有需要读到rowBatch中的一个stripe
    private boolean hasNextStripe;
    
    public StripeReaderImpl(EnhancedOrcReader fileReader, Reader.Options options) 
        throws IOException 
    {
        // 初始化必要的参数
        Configuration conf = fileReader.getConf();
        this.path = fileReader.getPath();
        this.maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(conf);
        OrcFile.WriterVersion writerVersion = fileReader.getWriterVersion();
        boolean ignoreNonUtf8BloomFilter = OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(conf);
        Boolean zeroCopy = options.getUseZeroCopy();
        if (zeroCopy == null) {
            zeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(conf);
        }
        Boolean skipCorrupt = options.getSkipCorruptRecords();
        if (skipCorrupt == null) {
            skipCorrupt = OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf);
        }
        // this.totalRowCount = 0;
        this.stripes = new ArrayList<>();
        long offset = options.getOffset();
        long maxOffset = options.getMaxOffset();
        
        // 用于构建batchReader
        SchemaEvolution evolution = null;
        TypeDescription fileSchema = fileReader.getSchema();
        TypeDescription optionsSchema = options.getSchema();
        if (optionsSchema == null) {
            LOG.info("Reader schema not provided -- using file schema {}", fileSchema);
            evolution = new SchemaEvolution(fileSchema, null, options);
        } else {

            // Now that we are creating a record reader for a file, validate that
            // the schema to read is compatible with the file schema.
            //
            evolution = new SchemaEvolution(fileSchema, optionsSchema, options);
            if (LOG.isDebugEnabled() && evolution.hasConversion()) {
                LOG.debug("ORC file {} has data type conversion --\nreader schema: {}\nfile schema: {}"
                    , this.path.toString()
                    , optionsSchema.toString()
                    , fileSchema.toString());
            }
        }
        this.colsIncluded = evolution.getFileIncluded();
        
        // 生成stripeIncluded，满足searchArgument的条带才会被读取
        boolean[] stripeIncluded;
        List<StripeStatistics> stripeStats = fileReader.getStripeStatistics();
        stripeIncluded = pickStripes(options.getSearchArgument(),
            writerVersion, 
            stripeStats, 
            stripeStats.size(), 
            this.path,
            evolution);
        if (stripeIncluded == null) { // null表示读取所有stripe
            stripeIncluded = new boolean[stripeStats.size()];
            Arrays.fill(stripeIncluded, true);
        }
        
        List<StripeInformation> stripesInfo = fileReader.getStripes();
        for (int i= 0; i < stripesInfo.size(); ++i) {
            StripeInformation stripeInfo = stripesInfo.get(i);
            long stripeStart = stripeInfo.getOffset();
            if (stripeStart < maxOffset && stripeStart >= offset && stripeIncluded[i]) {
                stripes.add(stripeInfo);
                // this.totalRowCount += stripeInfo.getNumberOfRows();
            }
        }
        
        // 构建filterCallBack，用于构建batchReader
        String[] filterCols = null;
        Consumer<OrcFilterContext> filterCallBack = null;
        String filePath = options.allowPluginFilters() ?
            fileReader.getFileSystem().makeQualified(path).toString() : null;
        BatchFilter filter = FilterFactory.createBatchFilter(options,
            evolution.getReaderBaseSchema(),
            evolution.isSchemaEvolutionCaseAware(),
            fileReader.getFileVersion(),
            false,
            filePath,
            conf);
        if (filter != null) {
            // If a filter is determined then use this
            filterCallBack = filter;
            filterCols = filter.getColumnNames();
        }
        // Map columnNames to ColumnIds
        boolean[] rowIndexCols = new boolean[evolution.getFileIncluded().length]; // 记录需要被filter筛选的列
        SortedSet<Integer> filterColIds = new TreeSet<>();
        if (filterCols != null) {
            for (String colName : filterCols) {
                TypeDescription expandCol = findColumnType(evolution, colName);
                // If the column is not present in the file then this can be ignored from read.
                if (expandCol == null || expandCol.getId() == -1) {
                    // Add -1 to filter columns so that the NullTreeReader is invoked during the LEADERS phase
                    filterColIds.add(-1);
                    // Determine the common parent and include these
                    expandCol = findMostCommonColumn(evolution, colName);
                }
                while (expandCol != null && expandCol.getId() != -1) {
                    // classify the column and the parent branch as LEAD
                    filterColIds.add(expandCol.getId());
                    rowIndexCols[expandCol.getId()] = true;
                    expandCol = expandCol.getParent();
                }
            }
            this.startReadPhase = TypeReader.ReadPhase.LEADERS;
            LOG.debug("Using startReadPhase: {} with filter columns: {}", startReadPhase, filterColIds);
        } else {
            this.startReadPhase = TypeReader.ReadPhase.ALL;
        }
        
        // 初始化dataReader
        if (options.getDataReader() != null) {
            this.dataReader = options.getDataReader().clone();
        } else {
            InStream.StreamOptions unencryptedOptions =
                InStream.options()
                    .withCodec(OrcCodecPool.getCodec(fileReader.getCompressionKind()))
                    .withBufferSize(fileReader.getCompressionSize());
            DataReaderProperties.Builder builder =
                DataReaderProperties.builder()
                    .withCompression(unencryptedOptions)
                    .withFileSystemSupplier(fileReader.getFileSystemSupplier())
                    .withPath(this.path)
                    .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
                    .withZeroCopy(zeroCopy)
                    .withMinSeekSize(options.minSeekSize())
                    .withMinSeekSizeTolerance(options.minSeekSizeTolerance());
            FSDataInputStream file = fileReader.takeFile();
            if (file != null) {
                builder.withFile(file);
            }
            this.dataReader = RecordReaderUtils.createDefaultDataReader(builder.build());
        }
        
        // 初始化batchReader
        ReaderEncryption encryption = fileReader.getEncryption();
        TreeReaderFactory.ReaderContext readerContext =
            new TreeReaderFactory.ReaderContext()
                .setSchemaEvolution(evolution)
                .setFilterCallback(filterColIds, filterCallBack)
                .skipCorrupt(skipCorrupt)
                .fileFormat(fileReader.getFileVersion())
                .useUTCTimestamp(fileReader.isUseUTCTimestamp())
                .setProlepticGregorian(fileReader.writerUsedProlepticGregorian(),
                    fileReader.getOptions().getConvertToProlepticGregorian())
                .setEncryption(encryption);
        reader = TreeReaderFactory.createRootReader(evolution.getReaderSchema(), readerContext);
        
        // 初始化stripePlanner
        planner = new StripePlanner(evolution.getFileSchema(), 
            encryption,
            dataReader, 
            writerVersion, 
            ignoreNonUtf8BloomFilter,
            maxDiskRangeChunkLimit, 
            filterColIds);
                
        // 将第一个includedStripe读入内存
        this.stripeIdx = 0;
        try {
            hasNextStripe = advanceToNextStripe();
        } catch (Exception e) {
            // Try to close since this happens in constructor.
            close();
            long stripeId = stripes.size() == 0 ? 0 : stripes.get(0).getStripeId();
            throw new IOException(String.format("Problem opening stripe %d footer in %s.",
                stripeId, path), e);
        }

    }
    
    private static TypeDescription findColumnType(SchemaEvolution evolution, String columnName) {
        try {
            TypeDescription readerColumn = evolution.getReaderBaseSchema()
                .findSubtype(columnName, evolution.isSchemaEvolutionCaseAware());
            return evolution.getFileType(readerColumn);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Filter could not find column with name: " +
                                                columnName + " on " + evolution.getReaderBaseSchema(),
                                                e);
        }
    }
      
    /**
     * Given a column name such as 'a.b.c', this method returns the column 'a.b.c' if present in the
     * file. In case 'a.b.c' is not found in file then it tries to look for 'a.b', then 'a'. If none
     * are present then it shall return null.
     *
     * @param evolution the mapping from reader to file schema
     * @param columnName the fully qualified column name to look for
     * @return the file column type or null in case none of the branch columns are present in the file
     * @throws IllegalArgumentException if the column was not found in the reader schema
     */
    private TypeDescription findMostCommonColumn(SchemaEvolution evolution, String columnName) {
        try {
            TypeDescription readerColumn = evolution.getReaderBaseSchema().findSubtype(
                columnName, evolution.isSchemaEvolutionCaseAware());
            TypeDescription fileColumn;
            do {
                fileColumn = evolution.getFileType(readerColumn);
                if (fileColumn == null) {
                    readerColumn = readerColumn.getParent();
                } else {
                    return fileColumn;
                }
            } while (readerColumn != null);
            return null;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Filter could not find column with name: " +
                                                columnName + " on " + evolution.getReaderBaseSchema(),
                                                e);
        }
    }
    
    // 读下一个需要读出文件的条带
    private boolean advanceToNextStripe() throws IOException {
        if (stripeIdx >= stripes.size()) {
            return false;
        }
        
        StripeInformation stripe = stripes.get(stripeIdx);
        clearStreams();
        planner.parseStripe(stripe, colsIncluded);
        // 读整个条带
        planner.readData(null, null, false, startReadPhase);
        reader.startStripe(planner, startReadPhase);
        // readedRowSize = 0;
        rowCountInStripe = stripe.getNumberOfRows();
        stripeIdx++;
        return true;
    }
    
    public static boolean[] pickStripes(SearchArgument sarg, 
        OrcFile.WriterVersion writerVersion,
        List<StripeStatistics> stripeStats,
        int stripeCount, Path filePath, 
        final SchemaEvolution evolution
    ) {
        if (sarg == null || stripeStats == null || writerVersion == OrcFile.WriterVersion.ORIGINAL) {
            return null; // only do split pruning if HIVE-8732 has been fixed in the writer
        }
        // eliminate stripes that doesn't satisfy the predicate condition
        List<PredicateLeaf> sargLeaves = sarg.getLeaves();
        int[] filterColumns = mapSargColumnsToOrcInternalColIdx(sargLeaves, evolution);
        return pickStripesInternal(sarg, filterColumns, stripeStats, stripeCount, filePath, evolution);
    }
    
    private static int[] mapSargColumnsToOrcInternalColIdx(
                            List<PredicateLeaf> sargLeaves,
                            SchemaEvolution evolution) {
        int[] result = new int[sargLeaves.size()];
        for (int i = 0; i < sargLeaves.size(); ++i) {
        int colNum = -1;
        try {
            String colName = sargLeaves.get(i).getColumnName();
            colNum = findColumns(evolution, colName);
        } catch (IllegalArgumentException e) {
            LOG.debug("{}", e.getMessage());
        }
        result[i] = colNum;
        }
        return result;
    }
    
    private static int findColumns(SchemaEvolution evolution,
                         String columnName) {
        TypeDescription fileColumn = findColumnType(evolution, columnName);
        return fileColumn == null ? -1 : fileColumn.getId();
    }
    
    
    private static boolean[] pickStripesInternal(SearchArgument sarg, int[] filterColumns,
        List<StripeStatistics> stripeStats, int stripeCount, Path filePath,
        final SchemaEvolution evolution) {
        boolean[] includeStripe = new boolean[stripeCount];
        for (int i = 0; i < includeStripe.length; ++i) {
            includeStripe[i] = (i >= stripeStats.size()) ||
                isStripeSatisfyPredicate(stripeStats.get(i), sarg, filterColumns, evolution);
            if (LOG.isDebugEnabled() && !includeStripe[i]) {
                LOG.debug("Eliminating ORC stripe-" + i + " of file '" + filePath
                    + "'  as it did not satisfy predicate condition.");
            }
        }
        return includeStripe;
    }
    
    private static boolean isStripeSatisfyPredicate(
        StripeStatistics stripeStatistics, SearchArgument sarg, int[] filterColumns,
        final SchemaEvolution evolution) {
        List<PredicateLeaf> predLeaves = sarg.getLeaves();
        TruthValue[] truthValues = new TruthValue[predLeaves.size()];
        for (int pred = 0; pred < truthValues.length; pred++) {
        if (filterColumns[pred] != -1) {
            if (evolution != null && !evolution.isPPDSafeConversion(filterColumns[pred])) {
            truthValues[pred] = TruthValue.YES_NO_NULL;
            } else {
                // column statistics at index 0 contains only the number of rows
                ColumnStatistics stats = stripeStatistics.getColumnStatistics()[filterColumns[pred]];
                // if row count is 0 and where there are no nulls it means index is disabled and we don't have stats
                if (stats.getNumberOfValues() == 0 && !stats.hasNull()) {
                    truthValues[pred] = TruthValue.YES_NO_NULL;
                    continue;
                }
                PredicateLeaf leaf = predLeaves.get(pred);
                try {
                    truthValues[pred] = RecordReaderImpl.evaluatePredicate(stats, leaf, null);
                } catch (NoDynamicValuesException dve) {
                    LOG.debug("Dynamic values are not available here {}", dve.getMessage());
                    boolean hasNulls = stats.hasNull() || leaf.getOperator() != Operator.NULL_SAFE_EQUALS;
                    truthValues[pred] = hasNulls ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
                }
            }
        } else {

            // parition column case.
            // partition filter will be evaluated by partition pruner so
            // we will not evaluate partition filter here.
            truthValues[pred] = TruthValue.YES_NO_NULL;
        }
        }
        return sarg.evaluate(truthValues).isNeeded();
    }
    
    // 一次读一个stripe，没有可读的stripe就返回false
    @Override
    public boolean nextBatch(VectorizedRowBatch batch) throws IOException{
        int batchSize = 0;
        try {
            if (!hasNextStripe) {
                batch.size = 0;
                return false;
            }
            batchSize = (int) Math.min(batch.getMaxSize(), rowCountInStripe);
            reader.setVectorColumnCount(batch.getDataColumnCount());
            reader.nextBatch(batch, batchSize, startReadPhase);
            if (startReadPhase == TypeReader.ReadPhase.LEADERS && batch.size > 0) {
                // At least 1 row has been selected and as a result we read the follow columns into the
                // row batch
                // 读那些不会被sarg筛选的列，rgIdx传入0是指从第一个rowGroup即从头开始读
                planner.readFollowData(null, null, 0, false);
                reader.startStripe(planner, TypeReader.ReadPhase.FOLLOWERS);
                reader.nextBatch(batch, batchSize,TypeReader.ReadPhase.FOLLOWERS);
            }
        } catch (IOException e) {
            // Rethrow exception with file name in log message
            throw new IOException("Error reading file: " + path, e);
        }
        batch.size = batchSize;
        batch.selectedInUse = false;
        // readedRowSize += rowCountInStripe;
        hasNextStripe = advanceToNextStripe();
        return batchSize != 0;
    }
    
    @Override
    public void close() throws IOException {
        clearStreams();
        dataReader.close();
    }
    
    private void clearStreams() {
        planner.clearStreams();
    }
    
}
