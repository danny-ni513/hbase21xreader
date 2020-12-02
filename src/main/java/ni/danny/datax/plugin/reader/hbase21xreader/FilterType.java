package ni.danny.datax.plugin.reader.hbase21xreader;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

public enum FilterType {
    CONST("const"),
    DATE("date")
    ;
    private  String typeName;

    FilterType(String typeName) {
        this.typeName = typeName;
    }

    public static FilterType getByTypeName(String typeName) {
        if(StringUtils.isBlank(typeName)){
            throw DataXException.asDataXException(Hbase21xReaderErrorCode.ILLEGAL_VALUE,
                    String.format("Hbasereader 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
        }
        for (FilterType filterType : values()) {
            if (StringUtils.equalsIgnoreCase(filterType.typeName, typeName.trim())) {
                return filterType;
            }
        }

        throw DataXException.asDataXException(Hbase21xReaderErrorCode.ILLEGAL_VALUE,
                String.format("Hbasereader 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
    }

    @Override
    public String toString() {
        return this.typeName;
    }

}
