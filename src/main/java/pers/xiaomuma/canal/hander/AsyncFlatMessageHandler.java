package pers.xiaomuma.canal.hander;


import com.alibaba.otter.canal.protocol.FlatMessage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;


public class AsyncFlatMessageHandler extends AbstractFlatMessageHandler {

    private ExecutorService executor;

    public AsyncFlatMessageHandler(List<? extends EntryHandler> entryHandlers,
                                   RowDataHandler<List<Map<String, String>>> rowDataHandler,
                                   ExecutorService executor) {
        super(entryHandlers, rowDataHandler);
        this.executor = executor;
    }

    @Override
    public void handleMessage(FlatMessage message) {
        executor.execute(() -> super.handleMessage(message));
    }


}
