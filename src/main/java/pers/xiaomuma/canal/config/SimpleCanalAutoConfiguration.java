package pers.xiaomuma.canal.config;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.yxbl.canal.base.client.SimpleCanalClient;
import com.yxbl.canal.base.factory.EntryColumnFactory;
import com.yxbl.canal.base.hander.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.util.List;
import java.util.concurrent.ExecutorService;

@ConditionalOnBean(value = {EntryHandler.class})
@Import(ThreadPoolConfiguration.class)
public class SimpleCanalAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "spring.canal")
    public SimpleCanalProperties simpleCanalProperties() {
        return new SimpleCanalProperties();
    }

    @Bean
    public RowDataHandler<CanalEntry.RowData> rowDataHandler() {
        return new AbstractRowDataHandler(new EntryColumnFactory());
    }

    @Bean
    @ConditionalOnProperty(value = "spring.canal.async", havingValue = "true", matchIfMissing = true)
    public MessageHandler messageHandler(RowDataHandler<CanalEntry.RowData> rowDataHandler, List<EntryHandler> entryHandlers,
                                         ExecutorService executorService) {
        return new AsyncMessageHandler(entryHandlers, rowDataHandler, executorService);
    }

    @Bean
    @ConditionalOnProperty(value = "spring.canal.async", havingValue = "false")
    public MessageHandler messageHandler(RowDataHandler<CanalEntry.RowData> rowDataHandler, List<EntryHandler> entryHandlers) {
        return new SyncMessageHandler(entryHandlers, rowDataHandler);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public SimpleCanalClient simpleCanalClient(@Qualifier("messageHandler") MessageHandler messageHandler,
                                               SimpleCanalProperties properties) {
        String addr = properties.getAddr();
        String[] array = addr.split(":");
        return SimpleCanalClient.Builder.builder()
                .hostName(array[0])
                .port(Integer.parseInt(array[1]))
                .destination(properties.getDestination())
                .userName(properties.getUsername())
                .password(properties.getPassword())
                .messageHandler(messageHandler)
                .batchSize(properties.getBatchSize())
                .filter(properties.getFilter())
                .timeout(properties.getTimeout())
                .unit(properties.getUnit())
                .build();

    }
}
