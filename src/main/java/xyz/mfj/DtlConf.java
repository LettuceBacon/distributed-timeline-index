package xyz.mfj;

public class DtlConf {
    // 应用于表的hadoop任务的最大内存限制，例如表的loadData任务
    public static final String MAX_TASK_MEM = "tbl.task-mem.max";
    
    // 表的hadoop任务涉及到的所有有效时间
    public static final String APPLICATION_PERIOD = "tbl.application-period";
    
    // 时间旅行任务的containsTime
    public static final String CONTAINS_TIME = "time-travel.contains-time";
    
    // 时态分组查询任务的startTime和endTime
    public static final String OVERLAPS_START_TIME = "temporal-grouping.start-time";
    public static final String OVERLAPS_END_TIME = "temporal-grouping.end-time";
    public static final String WINDOW_LENGTH = "temporal-grouping.windows-length";
    
    // 时态查询任务的时间段端点的类型
    public static final String PERIOD_ENDPOINT_TYPE = "temporal-query.period-endpoint-type";
    
    public static final String WHERE_EXPR = "where.expr";
    public static final String SELECT_EXPR = "select.expr";
    public static final String SELECT_TEMPORAL_AGGS = "select.temporal-aggs-name";
}
