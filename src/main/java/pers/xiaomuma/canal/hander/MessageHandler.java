package pers.xiaomuma.canal.hander;


@FunctionalInterface
public interface MessageHandler<T> {

    void handleMessage(T message);
}
