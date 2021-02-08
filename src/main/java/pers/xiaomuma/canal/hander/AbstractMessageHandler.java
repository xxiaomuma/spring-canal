package pers.xiaomuma.canal.hander;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.yxbl.canal.base.context.CanalContext;
import com.yxbl.canal.base.model.CanalMessage;
import com.yxbl.canal.base.utils.HandlerUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class AbstractMessageHandler implements MessageHandler<Message> {

    private Map<String, EntryHandler> entryHandlerMap;

    private RowDataHandler<CanalEntry.RowData> rowDataHandler;

    public AbstractMessageHandler(List<? extends EntryHandler> entryHandlers, RowDataHandler<CanalEntry.RowData> rowDataHandler) {
        this.entryHandlerMap = HandlerUtils.getTableHandlerMap(entryHandlers);
        this.rowDataHandler = rowDataHandler;
    }

    @Override
    public void handleMessage(Message message) {
        List<CanalEntry.Entry> entries = message.getEntries();
        Iterator<CanalEntry.Entry> iterator = entries.iterator();
        while (true) {
            CanalEntry.Entry entry;
            do {
                if (!iterator.hasNext()) {
                    return;
                }
                entry = iterator.next();
            } while(!entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA));
            try {
                EntryHandler<?> entryHandler = HandlerUtils.getEntryHandler(entryHandlerMap, entry.getHeader().getTableName());
                if(entryHandler == null){
                    return;
                }
                CanalMessage canalMessage = new CanalMessage();
                canalMessage.setId(message.getId());
                canalMessage.setTable(entry.getHeader().getTableName());
                canalMessage.setExecuteTime(entry.getHeader().getExecuteTime());
                canalMessage.setDatabase(entry.getHeader().getSchemaName());
                CanalContext.put(canalMessage);
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                CanalEntry.EventType eventType = rowChange.getEventType();
                for (CanalEntry.RowData rowData : rowDataList) {
                    rowDataHandler.handlerRowData(rowData,entryHandler,eventType);
                }
            } catch (Exception e) {
                throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
            } finally {
                CanalContext.remove();
            }
        }
    }
}
