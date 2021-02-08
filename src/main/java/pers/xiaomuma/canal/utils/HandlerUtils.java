package pers.xiaomuma.canal.utils;


import ch.qos.logback.classic.db.names.TableName;
import pers.xiaomuma.canal.annotation.CanalTableName;
import pers.xiaomuma.canal.constant.UndefinedTableName;
import pers.xiaomuma.canal.hander.EntryHandler;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class HandlerUtils {

    public static Map<String, EntryHandler> getTableHandlerMap(List<? extends EntryHandler> entryHandlers) {
        Map<String, EntryHandler> handlerMap = new ConcurrentHashMap<>();
        if (entryHandlers != null && entryHandlers.size() > 0) {
            for (EntryHandler handler : entryHandlers) {
                String canalTableName = getCanalTableName(handler);
                if (canalTableName != null) {
                    handlerMap.putIfAbsent(canalTableName.toLowerCase(), handler);
                }
            }
        }
        return handlerMap;
    }

    public static String getCanalTableName(EntryHandler entryHandler) {
        CanalTableName canalTable = entryHandler.getClass().getAnnotation(CanalTableName.class);
        if (canalTable != null) {
            return canalTable.value();
        }
        return null;
    }

    public static EntryHandler getEntryHandler(Map<String, EntryHandler> handlerMap, String tableName) {
        EntryHandler entryHandler = handlerMap.get(tableName);
        if (entryHandler == null) {
            return handlerMap.get(UndefinedTableName.ALL.name().toLowerCase());
        }
        return entryHandler;
    }

}
