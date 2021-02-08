package pers.xiaomuma.canal;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.commons.lang3.StringUtils;
import pers.xiaomuma.canal.hander.MessageHandler;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;



public class SimpleCanalClient extends AbstractCanalClient {

    public SimpleCanalClient(MessageHandler messageHandler, CanalConnector connector) {
        super(messageHandler, connector);
    }

    public static Builder builder(){
        return Builder.builder();
    }

    public static class Builder {
        private String filter = StringUtils.EMPTY;
        private Integer batchSize = 1;
        private Long timeout = 1L;
        private TimeUnit unit = TimeUnit.SECONDS;
        private String hostName;
        private Integer port;
        private String destination;
        private String userName;
        private String password;
        private MessageHandler messageHandler;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder hostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public Builder port(Integer port) {
            this.port = port;
            return this;
        }

        public Builder destination(String destination) {
            this.destination = destination;
            return this;
        }

        public Builder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }


        public Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public Builder batchSize(Integer batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder unit(TimeUnit unit) {
            this.unit = unit;
            return this;
        }

        public Builder messageHandler(MessageHandler messageHandler) {
            this.messageHandler = messageHandler;
            return this;
        }

        public SimpleCanalClient build() {
            CanalConnector connector = CanalConnectors
                    .newSingleConnector(new InetSocketAddress(hostName, port), destination, userName, password);
            SimpleCanalClient simpleCanalClient = new SimpleCanalClient(messageHandler, connector);
            simpleCanalClient.filter = this.filter;
            simpleCanalClient.unit = this.unit;
            simpleCanalClient.batchSize = this.batchSize;
            simpleCanalClient.timeout = this.timeout;
            return simpleCanalClient;
        }

    }
}
