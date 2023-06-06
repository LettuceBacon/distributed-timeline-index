package xyz.mfj.dataQuery.temporalGroupingQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.Library;
import xyz.mfj.dataDefiniation.ApplicationPeriod;
import xyz.mfj.dataDefiniation.OrcTable;
import xyz.mfj.dataQuery.DtlExpression;
import xyz.mfj.utils.TypeUtil;


public class TemporalGroupingQueryExecutorBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TemporalGroupingQueryExecutorBuilder.class);
    
    private OrcTable orcTable;
    private Object startTime;
    private Object endTime;
    private ApplicationPeriod appPrd;
    private DtlExpression whereExpr;
    // 给条带过滤器添加where语句中可用于过滤条带的sarg的处理器
    private Consumer<SearchArgument.Builder> whereSargAppender;
    private List<DtlExpression> selectExprs;
    private List<Text> selectNames;
    private List<Text> selectTypeDescs;
    private List<Text> aggregatorNames;
    private Integer numReduce;
    
    public TemporalGroupingQueryExecutorBuilder() {}
    
    public TemporalGroupingQueryExecutorBuilder fromTable(String tableName) {
        this.orcTable = (OrcTable)Library.getInstance().getTableByName(tableName);
        if (orcTable == null) {
            LOG.error("There doesn't exist a table {}", tableName);
            System.exit(1);
        }
        if (!orcTable.isTemporal()) {
            LOG.error("Table is non-temporal!");
            System.exit(1);
        }
        return this;
    }
    
    public TemporalGroupingQueryExecutorBuilder groupByPeriodOverlaps(
        String appPrdName, Object startTime, Object endTime
    ) {
        if (this.orcTable == null) {
            LOG.error("PERIOD CONTAINS should come after FROM TABLE!");
            System.exit(1);
        }
        appPrd = orcTable.getAppPrd(appPrdName);
        if (appPrd == null)
        {
            LOG.error("Table doesn't contains {}!", appPrdName);
            System.exit(1);
        }
        if (startTime != null && endTime != null
            && (!TypeUtil.isDateTime(startTime) || !TypeUtil.isDateTime(endTime)) )
        {
            LOG.error("OVERLAPS clause receive startTime with type {} and endTime with type {}, but require java.sql.TimeStamp or java.time.LocalDate", startTime.getClass(), endTime.getClass());
            System.exit(1);
        }
        this.startTime = startTime;
        this.endTime = endTime;
            
        return this;
    }
    
    public TemporalGroupingQueryExecutorBuilder withWhereClause(
        DtlExpression whereExpr,
        Consumer<SearchArgument.Builder> whereSargAppender
    )
    {
        this.whereExpr = whereExpr;
        this.whereSargAppender = whereSargAppender;
        return this;
    }
    
    public TemporalGroupingQueryExecutorBuilder withSelectClause(
        DtlExpression selectExpr,
        Text selectName,
        Text selectTypeDesc,
        Text aggregatorName
    )  {
        if (selectExprs == null) {
            selectExprs = new ArrayList<>();
            selectNames = new ArrayList<>();
            selectTypeDescs = new ArrayList<>();
            aggregatorNames = new ArrayList<>();
        }
        selectExprs.add(selectExpr);
        selectNames.add(selectName);
        selectTypeDescs.add(selectTypeDesc);
        aggregatorNames.add(aggregatorName);
        
        return this;
    }
    
    public TemporalGroupingQueryExecutorBuilder withParallelNum(int n) {
        this.numReduce = n;
        return this;
    }
    
    public TemporalGroupingQueryExecutor build() {
        if (orcTable == null 
            || selectExprs == null
            || startTime == null
            || endTime == null
            || appPrd == null
        ) {
            LOG.error("Temporal grouping query lacks key clause(SELECT, FROM, GROUP BY)!");
            System.exit(1);
        }
        
        return new TemporalGroupingQueryExecutor(orcTable,
            appPrd,
            whereExpr,
            whereSargAppender,
            selectExprs.toArray(new DtlExpression[0]),
            selectNames.toArray(new Text[0]),
            selectTypeDescs.toArray(new Text[0]),
            aggregatorNames != null ? aggregatorNames.toArray(new Text[0]) : null,
            startTime,
            endTime,
            numReduce);
    }
}
