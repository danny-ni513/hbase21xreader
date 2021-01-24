package ni.danny.datax.plugin.reader.hbase21xreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/**
 * @author bingobing
 * 抽取多版本
 */
public class MultiVersionTask extends HbaseAbstractTask{
    private final static Logger LOG = LoggerFactory.getLogger(MultiVersionTask.class);

    private int maxVersion;

    private List<Map> column;
    private List<HbaseColumnCell> hbaseColumnCells;

    public MultiVersionTask(Configuration configuration) {
        super(configuration);
        this.maxVersion = configuration.getInt(Key.MAX_VERSION);
        this.column = configuration.getList(Key.COLUMN,Map.class);
        this.hbaseColumnCells = Hbase21xHelper.parseColumn(this.column);
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
            Long columnValueFirstTimpstamp=0L;
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
                        String finalColumnName = columnName;
                        Hbase21xCell finalCell = result.listCells().stream().map(resultCell ->new Hbase21xCell(resultCell))
                                .filter(hbase21xCell -> finalColumnName.equals(hbase21xCell.getColumnName()))
                                .sorted(Comparator.comparing(Hbase21xCell::getTimestamp).reversed()).sorted(Comparator.comparing(Hbase21xCell::getTimestamp).reversed()).reduce(null,(a,b)->{
                                    if(a==null){
                                        return b;
                                    }else if(b!=null &&Bytes.compareTo(CellUtil.cloneValue(a.getCell()),CellUtil.cloneValue(b.getCell()))==0){
                                        return b;
                                    }else{
                                        return a;
                                    }
                                });

                        hbaseColumnValue = CellUtil.cloneValue(finalCell.getCell());
                        columnValueFirstTimpstamp = finalCell.getTimestamp();


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
                    //自动增加两列：TIMESTAMP & 转成日期时间类型的数据--STRING类型
                    if(cell.getWithVersionInfo()){
                        record.addColumn(new LongColumn(columnValueFirstTimpstamp));
                        record.addColumn(new StringColumn(new DateTime(columnValueFirstTimpstamp).toString(cell.getDateformat())));
                    }
                }
            }
        }catch (Exception e){
            record.setColumn(0,new StringColumn(Bytes.toStringBinary(result.getRow())));
            throw e;
        }

        return true;
    }

    public void setMaxVersion(Scan scan){
        if(this.maxVersion == -1 || this.maxVersion == Integer.MAX_VALUE){
            scan.readAllVersions();
        }else{
            scan.readVersions(this.maxVersion);
        }
    }

    @Override
    public void initScan(Scan scan) {
        this.hbaseColumnCells.forEach(cell->{
            String columnName = cell.getColumnName();
            if(!Hbase21xHelper.isRowkeyColumn(columnName)){
                this.scan.addColumn(cell.getColumnFamily(),cell.getQualifier());
            }
        });
        this.setMaxVersion(scan);
    }
}
