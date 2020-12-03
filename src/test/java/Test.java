import ni.danny.datax.plugin.reader.hbase21xreader.ColumnType;
import ni.danny.datax.plugin.reader.hbase21xreader.Hbase21xHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) throws Exception {
        int str=-1123;

        byte[] a = HConstants.EMPTY_BYTE_ARRAY;
        byte[] b = Hbase21xHelper.convertFilterToBytesAssignType(ColumnType.INT,"");
        System.out.println(Arrays.equals(a,b));
    }
}
