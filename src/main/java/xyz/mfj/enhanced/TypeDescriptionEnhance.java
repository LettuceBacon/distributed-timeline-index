package xyz.mfj.enhanced;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.orc.TypeDescription;

public class TypeDescriptionEnhance {
    // 表构造好后，模式不再变化

    // 与orc的TypeDescription类型使用不同的抽象：
    // class TypeDescription
    //     自己的id
    //     自己的名称（通过parent获取，没有parent或者parent没有fieldname则为null）
    //     自己的类型
    //     父节点索引
    //     孩子节点
    
    public static TypeDescription structAddField(TypeDescription struct,
        String fieldName,
        TypeDescription fieldType
    ) {
        struct.addField(fieldName, fieldType);
        fieldType.getId(); // 初始化id
        return struct;
    }
    
    /**
     * 从一个TypeDescription中找到id对应的名称和类型
     * @param schema
     * @param id
     * @return id如果在[schema.id, schema.maxId]之间返回对应名称和类型，否则返回null
     */
    public static Pair<String, TypeDescription> getNameAndType(TypeDescription schema, int id) {
        if (id < schema.getId() || id > schema.getMaximumId()) {
            // 要找的id不包含在schema里
            return null;
        }
        else if (id == schema.getId()) {
            // 当前schema就是要找的，需要去parent里获取名称
            TypeDescription parent = schema.getParent();
            if (parent == null) {
                return Pair.of(null, schema);
            }
            Pair<String, TypeDescription> result = null;
            List<String> names = parent.getFieldNames();
            List<TypeDescription> children = parent.getChildren();
            for (int i = 0; i < children.size(); i++) {
                if (children.get(i).getId() == id) {
                    result = Pair.of(
                        names == null ? null : names.get(i),
                        schema
                    );
                    break;
                }
            }
            return result;
        }
        else {
            Pair<String, TypeDescription> result = null;
            List<TypeDescription> children = schema.getChildren();
            if (children != null) {
                for (TypeDescription child : schema.getChildren()) {
                    result = getNameAndType(child, id);
                    if (result != null) {
                        break;
                    }
                }
            }
            return result;
        }
        
    }
}
