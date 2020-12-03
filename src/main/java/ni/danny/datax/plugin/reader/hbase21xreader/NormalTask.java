package ni.danny.datax.plugin.reader.hbase21xreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class NormalTask extends HbaseAbstractTask {
    private final static Logger LOG = LoggerFactory.getLogger(NormalTask.class);

    private List<Map> column;
    private List<HbaseColumnCell> hbaseColumnCells;

    public NormalTask(Configuration configuration) {
        super(configuration);
        this.column = configuration.getList(Key.COLUMN, Map.class);
        this.hbaseColumnCells = Hbase21xHelper.parseColumnOfNormalMode(this.column);
    }

    @Override
    public void initScan(Scan scan) {
        boolean isConstant;
        boolean isRowkeyColumn;
        for(HbaseColumnCell cell:this.hbaseColumnCells){
            isConstant = cell.isConstant();
            isRowkeyColumn = Hbase21xHelper.isRowkeyColumn(cell.getColumnName());
            if(!isConstant && !isRowkeyColumn){
                this.scan.addColumn(cell.getColumnFamily(), cell.getQualifier());
            }
        }
    }

    @Override
    public boolean fetchLine(Record record) throws Exception {
        Result result = super.getNextHbaseRow();
        if(null == result){
            return false;
        }
        super.lastResult = result;
        try{
            byte[] hbaseColumnValue;
            String columnName;
            ColumnType columnType;

            byte[] columnFamily;
            byte[] qualifier;

            for(HbaseColumnCell cell: this.hbaseColumnCells){
                columnType = cell.getColumnType();
                if(cell.isConstant()){
                    String constantValue = cell.getColumnValue();
                    Column constantColumn = super.convertValueToAssignType(columnType,constantValue,cell.getDateformat());
                    record.addColumn(constantColumn);
                }else{

                    columnName = cell.getColumnName();
                    if(Hbase21xHelper.isRowkeyColumn(columnName)){
                        hbaseColumnValue = result.getRow();
                        if(hbaseColumnValue == null|| hbaseColumnValue.length<=0){
                            LOG.warn("NOWROWKEY====>"+Bytes.toString(hbaseColumnValue));
                            LOG.warn("ROWKEY IS EMPTY");
                        }
                    }else{
                        columnFamily =cell.getColumnFamily();
                        qualifier = cell.getQualifier();
                        hbaseColumnValue = result.getValue(columnFamily,qualifier);

                        if( !Arrays.equals(HConstants.EMPTY_BYTE_ARRAY,cell.getFilterValue())
                                &&Arrays.equals(cell.getFilterValue(),hbaseColumnValue)){
                            hbaseColumnValue = HConstants.EMPTY_BYTE_ARRAY;
                        }

                        if( !Arrays.equals(HConstants.EMPTY_BYTE_ARRAY,cell.getDefaultValue())
                                && Arrays.equals(HConstants.EMPTY_BYTE_ARRAY,hbaseColumnValue)){
                            hbaseColumnValue = cell.getDefaultValue();
                        }
                    }

                    Column hbaseColumn = super.convertBytesToAssignType(columnType,hbaseColumnValue,cell.getDateformat());
                    record.addColumn(hbaseColumn);
                }
            }

        }catch (Exception e){
            record.setColumn(0,new StringColumn(Bytes.toStringBinary(result.getRow())));
            throw e;
        }
        return true;
    }


}
