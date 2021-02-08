package pers.xiaomuma.canal.hander;

import com.alibaba.otter.canal.protocol.CanalEntry;

@FunctionalInterface
public interface RowDataHandler<T> {

    <R> void handlerRowData(T t, EntryHandler<R> handler, CanalEntry.EventType eventType) throws Exception;
}
