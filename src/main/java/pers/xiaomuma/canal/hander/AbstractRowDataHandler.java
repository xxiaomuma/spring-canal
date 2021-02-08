package pers.xiaomuma.canal.hander;

import com.alibaba.otter.canal.protocol.CanalEntry;
import pers.xiaomuma.canal.factory.IColumnFactory;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class AbstractRowDataHandler implements RowDataHandler<CanalEntry.RowData> {

    private IColumnFactory<List<CanalEntry.Column>> columnFactory;

    public AbstractRowDataHandler(IColumnFactory<List<CanalEntry.Column>> columnFactory) {
        this.columnFactory = columnFactory;
    }

    @Override
    public <R> void handlerRowData(CanalEntry.RowData rowData, EntryHandler<R> handler, CanalEntry.EventType eventType) throws Exception {
        if (handler == null) {
            return;
        }
        switch (eventType) {
            case INSERT:
                R insertObject = columnFactory.newInstance(handler, rowData.getAfterColumnsList());
                handler.insert(insertObject);
                break;
            case UPDATE:
                Set<String> updateColumnSet = rowData.getAfterColumnsList().stream().filter(CanalEntry.Column::getUpdated)
                        .map(CanalEntry.Column::getName).collect(Collectors.toSet());
                R before = columnFactory.newInstance(handler, rowData.getBeforeColumnsList(),updateColumnSet);
                R after = columnFactory.newInstance(handler, rowData.getAfterColumnsList());
                handler.update(before, after);
                break;
            case DELETE:
                R  deleteObject = columnFactory.newInstance(handler, rowData.getBeforeColumnsList());
                handler.delete(deleteObject);
                break;
            default:
                break;
        }
    }
}
