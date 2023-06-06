package xyz.mfj.dataDefiniation;

public class RowVector {
    private Object[] values;
    
    public RowVector(int size) {
        this.values = new Object[size];
    }
    
    public RowVector(Object ... values) {
        this.values = values;
    }
    
    public Object get(int i) {
        return values[i];
    }
    
    public void set(int i, Object value) {
        values[i] = value;
    }
    
    public int size() {
        return values.length;
    }
}
