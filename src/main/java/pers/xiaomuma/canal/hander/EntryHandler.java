package pers.xiaomuma.canal.hander;

public interface EntryHandler<T> {

    default void insert(T t) {

    }

    default void update(T before, T after) {

    }

    default void delete(T t) {

    }
}
