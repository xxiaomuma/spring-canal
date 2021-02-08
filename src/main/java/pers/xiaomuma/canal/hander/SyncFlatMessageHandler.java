package pers.xiaomuma.canal.hander;

import com.alibaba.otter.canal.protocol.FlatMessage;

import java.util.List;
import java.util.Map;

public class SyncFlatMessageHandler extends AbstractFlatMessageHandler {


    public SyncFlatMessageHandler(List<? extends EntryHandler> entryHandlers, RowDataHandler<List<Map<String, String>>> rowDataHandler) {
        super(entryHandlers, rowDataHandler);
    }

    @Override
    public void handleMessage(FlatMessage message) {
        super.handleMessage(message);
    }
}
