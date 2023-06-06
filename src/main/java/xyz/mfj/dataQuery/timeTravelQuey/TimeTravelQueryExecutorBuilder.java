package xyz.mfj.dataQuery.timeTravelQuey;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.Library;
import xyz.mfj.dataDefiniation.ApplicationPeriod;
import xyz.mfj.dataDefiniation.OrcTable;
import xyz.mfj.dataQuery.DtlExpression;
import xyz.mfj.utils.TypeUtil;

public class TimeTravelQueryExecutorBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TimeTravelQueryExecutorBuilder.class);
    
    private OrcTable orcTable;
    private Object containsTime;
    private ApplicationPeriod appPrd;
    private DtlExpression whereExpr;
    // 给条带过滤器添加where语句中可用于过滤条带的sarg的处理器
    private Consumer<SearchArgument.Builder> whereSargAppender;
    private List<DtlExpression> selectExprs;
    private List<Text> selectNames;
    private List<Text> selectTypeDescs;
    private List<Text> aggregatorNames;
    private List<Pair<String, String>> properties;
    private int selectNum;
    private int aggregatorNum;
    
    public TimeTravelQueryExecutorBuilder() {
        this.selectNum = 0;
        this.aggregatorNum = 0;
    }
    
    public TimeTravelQueryExecutorBuilder fromTable(String tableName) {
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
    
    public TimeTravelQueryExecutorBuilder periodContainsTime(String appPrdName, Object containsTime)
    {
        
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
        if (!TypeUtil.isDateTime(containsTime))
        {
            LOG.error("CONTAINS clause receive {}, but require java.sql.TimeStamp or java.time.LocalDate", containsTime.getClass());
            System.exit(1);
        }
        this.containsTime = containsTime;
            
        return this;
    }
    
    public TimeTravelQueryExecutorBuilder withWhereClause(
        DtlExpression whereExpr,
        Consumer<SearchArgument.Builder> whereSargAppender
    ) {
        this.whereExpr = whereExpr;
        this.whereSargAppender = whereSargAppender;
        return this;
    }
    
    public TimeTravelQueryExecutorBuilder withSelectClause(
        DtlExpression selectExpr,
        Text selectName,
        Text selectTypeDesc,
        Text aggregatorName
    ) {
        if (selectExprs == null) {
            selectExprs = new ArrayList<>();
            selectNames = new ArrayList<>();
            selectTypeDescs = new ArrayList<>();
        }
        selectExprs.add(selectExpr);
        selectNames.add(selectName);
        selectTypeDescs.add(selectTypeDesc);
        selectNum++;
        
        if (aggregatorName != null) {
            if (aggregatorNames == null) {
                aggregatorNames = new ArrayList<>();
            }
            aggregatorNames.add(aggregatorName);
            aggregatorNum++;
        }
        return this;
    }
    
    public TimeTravelQueryExecutorBuilder withProperty(String name, String value) {
        if (properties == null) {
            properties = new ArrayList<>();
        }
        properties.add(Pair.of(name, value));
        return this;
    }
    
    public TimeTravelQueryExecuter build() {
        if (orcTable == null 
            || appPrd == null
            || containsTime == null
            || selectExprs == null
        ) {
            LOG.error("Time travel query lacks key clause(SELECT, FROM, PERIOD CONTAINS)!");
            System.exit(1);
        }
        if (aggregatorNum != 0 && aggregatorNum != selectNum) {
            LOG.error("If one temporalAggregator is given, all other select clauses should have temporalAggregator");
            System.exit(1);
        }
        
        return new TimeTravelQueryExecuter(orcTable,
            appPrd,
            whereExpr,
            whereSargAppender,
            selectExprs.toArray(new DtlExpression[0]),
            selectNames.toArray(new Text[0]),
            selectTypeDescs.toArray(new Text[0]),
            aggregatorNames != null ? aggregatorNames.toArray(new Text[0]) : null,
            containsTime,
            properties);
        
    }
}