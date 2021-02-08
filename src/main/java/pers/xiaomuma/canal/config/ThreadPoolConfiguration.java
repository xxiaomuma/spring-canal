package pers.xiaomuma.canal.config;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import pers.xiaomuma.canal.hander.UncaughtExceptionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ConditionalOnProperty(value = "spring.canal.async", havingValue = "true", matchIfMissing = true)
public class ThreadPoolConfiguration {


    @Bean(destroyMethod = "shutdown")
    public ExecutorService executorService() {
        BasicThreadFactory factory = new BasicThreadFactory.Builder().namingPattern("canal-execute-thread-%d")
                .uncaughtExceptionHandler(new UncaughtExceptionHandler()).build();
        return Executors.newFixedThreadPool(10, factory);
    }
}
