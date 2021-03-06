package ni.danny.datax.plugin.reader.hbase21xreader;


import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public abstract class HbaseAbstractTask {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseAbstractTask.class);

    private byte[] startKey = null;
    private byte[] endKey = null;

    protected Table htable;
    protected String encoding;
    protected int scanLimitSize;
    protected int scanCacheSize;
    protected int scanBatchSize;
    protected Result lastResult = null;
    protected Scan scan;
    protected ResultScanner resultScanner;
    public HbaseAbstractTask(com.alibaba.datax.common.util.Configuration configuration) {

        this.htable = Hbase21xHelper.getTable(configuration);

        this.encoding = configuration.getString(Key.ENCODING,Constant.DEFAULT_ENCODING);

        this.scanCacheSize = configuration.getInt(Key.SCAN_CACHE_SIZE,Constant.DEFAULT_SCAN_CACHE_SIZE);
        this.scanBatchSize = configuration.getInt(Key.SCAN_BATCH_SIZE,Constant.DEFAULT_SCAN_BATCH_SIZE);
        this.scanLimitSize = configuration.getInt(Key.SCAN_LIMIT_SIZE,Constant.DEFAULT_SCAN_LIMIT);

        this.startKey = Hbase21xHelper.convertInnerStartRowkey(configuration);
        this.endKey =  Hbase21xHelper.convertInnerEndRowkey(configuration);

    }

    public abstract boolean fetchLine(com.alibaba.datax.common.element.Record record) throws Exception;

    //不同模式设置不同,如多版本模式需要设置版本
    public abstract void initScan(Scan scan);

    public void prepare() throws Exception {

        LOG.info("The task set startRowkey=[{}], endRowkey=[{}].", Bytes.toStringBinary(this.startKey), Bytes.toStringBinary(this.endKey));


        this.scan = new Scan();

        if(this.scanLimitSize>0){
            //每一次RPC请求返回的记录行数
            this.scan.setLimit(this.scanLimitSize);
        }

        this.scan.setReadType(Scan.ReadType.PREAD);

        this.scan.withStartRow(startKey);
        this.scan.withStopRow(endKey);


        //scan的Caching Batch全部留在hconfig中每次从服务器端读取的行数，设置默认值未256
        this.scan.setCaching(this.scanCacheSize);
        //设置获取记录的列个数，hbase默认无限制，也就是返回所有的列,这里默认是100
        this.scan.setBatch(this.scanBatchSize);
        //为是否缓存块，hbase默认缓存,同步全部数据时非热点数据，因此不需要缓存
        this.scan.setCacheBlocks(false);
        initScan(this.scan);

        this.resultScanner = this.htable.getScanner(this.scan);
    }

    public void close()  {
        Hbase21xHelper.closeResultScanner(this.resultScanner);
        Hbase21xHelper.closeTable(this.htable);
    }

    protected Result getNextHbaseRow() throws IOException {
        Result result;
        try {
            result = resultScanner.next();
        } catch (IOException e) {
            if (lastResult != null) {
                this.scan.withStartRow(lastResult.getRow());
            }
            resultScanner = this.htable.getScanner(scan);
            result = resultScanner.next();
            if (lastResult != null && Bytes.equals(lastResult.getRow(), result.getRow())) {
                result = resultScanner.next();
            }
        }
        lastResult = result;
        // may be null
        return result;
    }

    public Column convertBytesToAssignType(ColumnType columnType, byte[] byteArray, String dateformat) throws Exception {
        Column column;
        switch (columnType) {
            case BOOLEAN:
                column = new BoolColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toBoolean(byteArray));
                break;
            case SHORT:
                column = new LongColumn(ArrayUtils.isEmpty(byteArray) ? null : String.valueOf(Bytes.toShort(byteArray)));
                break;
            case INT:
                column = new LongColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toInt(byteArray));
                break;
            case LONG:
                column = new LongColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toLong(byteArray));
                break;
            case FLOAT:
                column = new DoubleColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toFloat(byteArray));
                break;
            case DOUBLE:
                column = new DoubleColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toDouble(byteArray));
                break;
            case STRING:
                column = new StringColumn(ArrayUtils.isEmpty(byteArray) ? null : new String(byteArray, encoding));
                break;
            case BINARY_STRING:
                column = new StringColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toStringBinary(byteArray));
                break;
            case DATE:
                String dateValue = Bytes.toStringBinary(byteArray);
                column = new DateColumn(ArrayUtils.isEmpty(byteArray) ? null : DateUtils.parseDate(dateValue, new String[]{dateformat}));
                break;
            default:
                throw DataXException.asDataXException(Hbase21xReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 不支持您配置的列类型:" + columnType);
        }
        return column;
    }

    public Column convertValueToAssignType(ColumnType columnType, String constantValue,String dateformat) throws Exception {
        Column column;
        switch (columnType) {
            case BOOLEAN:
                column = new BoolColumn(constantValue);
                break;
            case SHORT:
            case INT:
            case LONG:
                column = new LongColumn(constantValue);
                break;
            case FLOAT:
            case DOUBLE:
                column = new DoubleColumn(constantValue);
                break;
            case STRING:
                column = new StringColumn(constantValue);
                break;
            case DATE:
                column = new DateColumn(DateUtils.parseDate(constantValue, new String[]{dateformat}));
                break;
            default:
                throw DataXException.asDataXException(Hbase21xReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 常量列不支持您配置的列类型:" + columnType);
        }
        return column;
    }

}
