package pers.xiaomuma.canal.hander;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.util.List;
import java.util.concurrent.ExecutorService;


public class AsyncMessageHandler extends AbstractMessageHandler {

    private ExecutorService executor;

    public AsyncMessageHandler(List<? extends EntryHandler> entryHandlers,
                                   RowDataHandler<CanalEntry.RowData> rowDataHandler,
                                   ExecutorService executor) {
        super(entryHandlers, rowDataHandler);
        this.executor = executor;
    }

    @Override
    public void handleMessage(Message message) {
        executor.execute(() -> super.handleMessage(message));
    }
}
