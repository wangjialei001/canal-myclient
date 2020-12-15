package com.client;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.client.DB.DAO;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class ClientSample {
    public static void main(String[] args) throws InterruptedException {
        monitorServer();
    }
    private static  void monitorServer() throws InterruptedException {
        String hostname=PropertiesCfg.get("hostname");
        Integer port=Integer.parseInt(PropertiesCfg.get("port"));
        String destination=PropertiesCfg.get("destination");
        String topic=PropertiesCfg.get("topic");
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostname,
                port), destination, "", "");
//        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("10.0.102.147",
//                11111), "10.0.102.147_DB", "", "");
        int batchSize = 1000;
        int emptyCount = 0;
        long batchId=0;
        try {
            connector.connect();
            //connector.subscribe(".*\\..*");
            connector.subscribe(topic);
            connector.rollback();
            int totalEmptyCount = 120;
            while (emptyCount < totalEmptyCount) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    //System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    emptyCount = 0;
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

            System.out.println("empty too many times, exit");
        }catch (Exception e) {
            if(batchId > -1){
                connector.rollback(batchId);// 处理失败, 回滚数据
            }
        }finally {
            System.out.printf("mysql断开连接，1分钟后重新连接");
            Thread.sleep(1000 * 60);
            //connector.disconnect();
            //重新连接
            monitorServer();
        }
    }
    private static void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));
            String sql=rowChage.getSql();
            if(sql != null && sql.length() > 0){
                System.out.printf(sql);
                insertDB(sql);
            }

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    saveDeleteSql(entry);
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    saveInsertSql(entry);
                    printColumn(rowData.getAfterColumnsList());
                } else {
                    saveUpdateSql(entry);
                    System.out.println("-------&gt; before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------&gt; after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }
    //获取update  sql 语句
    private static void saveUpdateSql(Entry entry){
        try {
            RowChange rowChange=RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDatasList=rowChange.getRowDatasList();
            for (RowData rowData:rowDatasList){
                List<Column> newColumnList=rowData.getAfterColumnsList();
                StringBuffer sql=new StringBuffer("update "+ entry.getHeader().getSchemaName()+"."+entry.getHeader().getTableName()+" set ");
                for (int i = 0; i < newColumnList.size(); i++) {
                    if(!newColumnList.get(i).getUpdated()){
                        continue;
                    }
                    sql.append(" " + newColumnList.get(i).getName() +"=");
                    if(newColumnList.get(i).getMysqlType().startsWith("bigint")
                            || newColumnList.get(i).getMysqlType().startsWith("int")
                            || newColumnList.get(i).getMysqlType().startsWith("float")
                            || newColumnList.get(i).getMysqlType().startsWith("decimal")
                            || newColumnList.get(i).getMysqlType().startsWith("bool")
                            || newColumnList.get(i).getMysqlType().startsWith("bit")
                            || newColumnList.get(i).getMysqlType().startsWith("numeric")
                            || newColumnList.get(i).getMysqlType().startsWith("smallint")
                            || newColumnList.get(i).getMysqlType().startsWith("year")
                            || newColumnList.get(i).getMysqlType().startsWith("tinyint")
                            || newColumnList.get(i).getMysqlType().startsWith("mediumint")){
                        sql.append(newColumnList.get(i).getValue());
                    }else{
                        sql.append("'" + newColumnList.get(i).getValue() + "'");
                    }
                    if (i != newColumnList.size() - 1) {
                        sql.append(",");
                    }
                }
                StringBuffer sqlWhere= new StringBuffer();
                if(sql.toString().endsWith(",")){
                    sqlWhere.append(sql.toString().substring(0,sql.toString().length()-1));
                }else{
                    sqlWhere.append(sql.toString());
                }

                sqlWhere.append(" where ");
                List<Column> oldColumnList = rowData.getBeforeColumnsList();
                for (Column column : oldColumnList) {
                    if (column.getIsKey()) {
                        //暂时只支持单一主键
                        sqlWhere.append(column.getName() + "=" + column.getValue());
                        break;
                    }
                }
                insertDB(sqlWhere.toString());
                System.out.printf("更新数据库："+sqlWhere.toString());
            }
        }catch (InvalidProtocolBufferException e){
            e.printStackTrace();
        }
    }
    /*
    获取插入sql语句
     */
    private static  void saveInsertSql(Entry entry){
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDatasList = rowChange.getRowDatasList();
            for (RowData rowData : rowDatasList) {
                List<Column> columnList = rowData.getAfterColumnsList();
                StringBuffer sql = new StringBuffer("insert into " +entry.getHeader().getSchemaName()+'.'+ entry.getHeader().getTableName() + " (");
                for (int i = 0; i < columnList.size(); i++) {
                    sql.append(columnList.get(i).getName());
                    if (i != columnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(") VALUES (");
                for (int i = 0; i < columnList.size(); i++) {
                    //sql.append("'" + columnList.get(i).getValue() + "'");
                    if(columnList.get(i).getMysqlType().startsWith("bigint")
                            || columnList.get(i).getMysqlType().startsWith("int")
                            || columnList.get(i).getMysqlType().startsWith("float")
                            || columnList.get(i).getMysqlType().startsWith("decimal")
                            || columnList.get(i).getMysqlType().startsWith("bool")
                            || columnList.get(i).getMysqlType().startsWith("bit")
                            || columnList.get(i).getMysqlType().startsWith("numeric")
                            || columnList.get(i).getMysqlType().startsWith("smallint")
                            || columnList.get(i).getMysqlType().startsWith("year")
                            || columnList.get(i).getMysqlType().startsWith("tinyint")
                            || columnList.get(i).getMysqlType().startsWith("mediumint")){
                        sql.append(columnList.get(i).getValue());
                    }else{
                        sql.append("'" + columnList.get(i).getValue() + "'");
                    }
                    if (i != columnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(")");
                insertDB(sql.toString());
                System.out.printf("插入数据库："+sql.toString());
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
    /*
    获取删除sql语句
     */
    private static void saveDeleteSql(Entry entry) {
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDatasList = rowChange.getRowDatasList();
            for (RowData rowData : rowDatasList) {
                List<Column> columnList = rowData.getBeforeColumnsList();
                StringBuffer sql = new StringBuffer("delete from " + entry.getHeader().getTableName() + " where ");
                for (Column column : columnList) {
                    if (column.getIsKey()) {
                        //暂时只支持单一主键
                        sql.append(column.getName() + "=" + column.getValue());
                        break;
                    }
                }
                System.out.printf("删除数据库："+sql.toString());
                insertDB(sql.toString());
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
    private static void printColumn(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    private static void insertDB(String sql){
        if(sql!=null && sql.length()>0){
            new DAO().Update("insert into sqllog (body) values(?)", sql);
        }
    }
}
