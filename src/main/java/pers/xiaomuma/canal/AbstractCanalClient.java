package pers.xiaomuma.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.xiaomuma.canal.hander.MessageHandler;

import java.util.concurrent.TimeUnit;



public class AbstractCanalClient implements CanalClient {

    private Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);

    protected volatile boolean flag;
    protected String filter = StringUtils.EMPTY;
    protected Integer batchSize = 1;
    protected Long timeout = 1L;
    protected TimeUnit unit = TimeUnit.SECONDS;
    private MessageHandler messageHandler;
    private Thread workThread;
    private CanalConnector connector;

    public AbstractCanalClient(MessageHandler messageHandler, CanalConnector connector) {
        this.messageHandler = messageHandler;
        this.connector = connector;
    }

    @Override
    public void start() {
        logger.info("start canal client");
        workThread = new Thread(this::process);
        workThread.setName("canal-client-thread");
        flag = true;
        workThread.start();
    }

    @Override
    public void stop() {
        logger.info("stop canal client");
        flag = false;
        if (null != workThread) {
            workThread.interrupt();
        }
    }

    @Override
    public void process() {
        while (flag) {
            try {
                connector.connect();
                connector.subscribe(filter);
                long batchId;
                for(; this.flag; this.connector.ack(batchId)) {
                    Message message = this.connector.getWithoutAck(this.batchSize, this.timeout, this.unit);
                    batchId = message.getId();
                    if (logger.isDebugEnabled()) {
                        logger.info("获取消息 -> {}", message);
                    }
                    if (message.getId() != -1L && message.getEntries().size() != 0) {
                        messageHandler.handleMessage(message);
                    }
                }
            } catch (Exception e) {
                logger.error("canal client error", e);
            } finally {
                connector.disconnect();
            }
        }
    }
}
