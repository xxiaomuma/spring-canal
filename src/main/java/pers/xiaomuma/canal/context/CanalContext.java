package pers.xiaomuma.canal.context;


import pers.xiaomuma.canal.model.CanalMessage;

public class CanalContext {

    private static final ThreadLocal<CanalMessage> CONTEXT = new ThreadLocal<>();

    public static CanalMessage get() {
        return CONTEXT.get();
    }

    public static void put(CanalMessage message) {
        CONTEXT.set(message);
    }

    public static void remove() {
        CONTEXT.remove();
    }

}
