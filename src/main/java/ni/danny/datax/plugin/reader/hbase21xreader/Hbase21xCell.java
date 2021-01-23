package ni.danny.datax.plugin.reader.hbase21xreader;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

/**
 * @author bingobing
 */
public class Hbase21xCell {
    private String rowkey;
    private String columnName;
    private Long timestamp;
    private Cell cell;
    public Hbase21xCell(Cell cell){
        this.cell = cell;
        this.rowkey = new String(CellUtil.cloneRow(cell));
        this.columnName = new String(CellUtil.cloneFamily(cell))+":"+new String(CellUtil.cloneQualifier(cell));
        this.timestamp = cell.getTimestamp();

    }

    public String getRowkey() {
        return rowkey;
    }

    public String getColumnName() {
        return columnName;
    }
    public String getRowkeyAndColumnName(){
        return rowkey+"-"+columnName;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Cell getCell() {
        return cell;
    }
    @Override
    public String toString(){
        return "{rowkey:"+rowkey+",columnName:"+columnName+",timestamp:"+timestamp+",cell:"+cell.toString()+"}";
    }
}
