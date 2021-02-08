package pers.xiaomuma.canal.hander;


import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.yxbl.canal.base.context.CanalContext;
import com.yxbl.canal.base.model.CanalMessage;
import com.yxbl.canal.base.utils.HandlerUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AbstractFlatMessageHandler implements MessageHandler<FlatMessage>{

    private Map<String, EntryHandler> entryHandlerMap;

    private RowDataHandler<List<Map<String, String>>> rowDataHandler;

    public AbstractFlatMessageHandler(List<? extends EntryHandler> entryHandlers, RowDataHandler<List<Map<String, String>>> rowDataHandler) {
        this.entryHandlerMap = HandlerUtils.getTableHandlerMap(entryHandlers);
        this.rowDataHandler = rowDataHandler;
    }

    @Override
    public void handleMessage(FlatMessage message) {
        List<Map<String, String>> data = message.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        for (int i = 0; i < data.size(); i++) {
            CanalEntry.EventType eventType = CanalEntry.EventType.valueOf(message.getType());
            List<Map<String, String>> values;
            if (eventType.equals(CanalEntry.EventType.UPDATE)) {
                Map<String, String> map = data.get(i);
                Map<String, String> oldMap = message.getOld().get(i);
                values = Stream.of(map, oldMap).collect(Collectors.toList());
            } else {
                values = Stream.of(data.get(i)).collect(Collectors.toList());
            }
            try {
                EntryHandler<?> entryHandler = HandlerUtils.getEntryHandler(entryHandlerMap, message.getTable());
                if (entryHandler == null) {
                    return;
                }
                CanalMessage canalMessage = new CanalMessage();
                canalMessage.setId(message.getId());
                canalMessage.setTable(message.getTable());
                canalMessage.setExecuteTime(message.getEs());
                canalMessage.setDatabase(message.getDatabase());
                canalMessage.setCreateTime(message.getTs());
                CanalContext.put(canalMessage);
                rowDataHandler.handlerRowData(values, entryHandler, eventType);
            } catch (Exception e) {
                throw new RuntimeException("parse event has an error , data:" + data.toString(), e);
            } finally {
                CanalContext.remove();
            }

        }
    }
}
