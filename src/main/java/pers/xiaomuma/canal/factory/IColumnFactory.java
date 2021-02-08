package pers.xiaomuma.canal.factory;

import pers.xiaomuma.canal.hander.EntryHandler;
import java.util.Set;

public interface IColumnFactory<T> {

    <R> R newInstance(EntryHandler handler, T t) throws Exception;

    <R> R newInstance(EntryHandler handler, T t, Set<String> updateColumn) throws Exception;
}
