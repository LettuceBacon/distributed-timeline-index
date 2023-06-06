package xyz.mfj.dataDefiniation;

public class ApplicationPeriod {
    
    private String appPrdName;
    // 所有id都是不包含顶层struct的id
    private int appPrdSId;
    private int appPrdEId;
    private int rf0Id;
    private int rf1Id;
    private TimelineIndex index;
    
    public ApplicationPeriod() {}
    
    public ApplicationPeriod(
        String appPrdName,
        int appPrdSId,
        int appPrdEId,
        int rf0Id,
        int rf1Id,
        TimelineIndex index
    ) {
        this.appPrdName = appPrdName;
        this.appPrdSId = appPrdSId;
        this.appPrdEId = appPrdEId;
        this.rf0Id = rf0Id;
        this.rf1Id = rf1Id;
        this.index = index;
    }
    
    public String getAppPrdName() {
        return appPrdName;
    }
    
    public int getAppPrdSId() {
        return appPrdSId;
    }
    
    public int getAppPrdEId() {
        return appPrdEId;
    }
    
    public int getRf0Id() {
        return rf0Id;
    }
    
    public int getRf1Id() {
        return rf1Id;
    }
    
    public TimelineIndex getIndex() {
        return index;
    }
    
}
