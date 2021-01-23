import ni.danny.datax.plugin.reader.hbase21xreader.Hbase21xCell;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class Test {
    private final static Logger log = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create(); // co GetExample-1-CreateConf Create the configuration.
        conf.set("hbase.zookeeper.quorum", "192.168.8.2:2181");
        Connection hConnection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("DW:TEST");
        Table hTable = hConnection.getTable(tableName);
        Scan scan = new Scan();
        scan.readAllVersions();
        scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("age"));
        ResultScanner resultScanner = hTable.getScanner(scan);
        Result result = resultScanner.next();
        String[] columnNames = new String[2];
        columnNames[0]="cf:name";
        columnNames[1]="cf:age";
        String currentColumnName="";
        for(String columnName:columnNames){
            currentColumnName = columnName;
            String finalCurrentColumnName = currentColumnName;
            List<Hbase21xCell> sortedList = result.listCells().stream().map(cell ->new Hbase21xCell(cell))
                    // .collect(groupingBy(Hbase21xCell::getRowkeyAndColumnName))
                    .filter(hbase21xCell -> finalCurrentColumnName.equals(hbase21xCell.getColumnName()))
                    .sorted(Comparator.comparing(Hbase21xCell::getTimestamp).reversed()).collect(Collectors.toList());
            byte[] value=null;
            Long firstTime = 0L;
            for(Hbase21xCell hbase21xCell: sortedList){
                byte[] tmpValue = CellUtil.cloneValue(hbase21xCell.getCell());
                if(value==null){
                    value = tmpValue;
                    firstTime = hbase21xCell.getTimestamp();
                }else if(Bytes.compareTo(value,tmpValue)==0){
                    firstTime = hbase21xCell.getTimestamp();
                }else{
                    break;
                }
            }
            log.info("columnName={},value={};firstTime={},firstTimeStr={}",currentColumnName,new String(value),firstTime,new DateTime(firstTime).toString("yyyyMMdd HH:mm:ss.SSS"));
        }


    }
}

