package pers.xiaomuma.canal.factory;


import pers.xiaomuma.canal.constant.UndefinedTableName;
import pers.xiaomuma.canal.hander.EntryHandler;
import pers.xiaomuma.canal.utils.GenericUtils;
import pers.xiaomuma.canal.utils.HandlerUtils;

public abstract class AbstractColumnFactory<T> implements IColumnFactory<T> {

    @Override
    public <R> R newInstance(EntryHandler handler, T t) throws Exception {
        String canalTableName = HandlerUtils.getCanalTableName(handler);
        if (UndefinedTableName.ALL.name().toLowerCase().equals(canalTableName)) {
            return (R) t;
        }
        Class<R> tableClazz = GenericUtils.getTableClass(handler);
        if (tableClazz != null) {
            return newInstance(tableClazz, t);
        }
        return null;
    }

    abstract public <R> R newInstance(Class<R> clazz, T t) throws Exception;
}
