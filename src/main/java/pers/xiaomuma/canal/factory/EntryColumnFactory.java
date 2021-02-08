package pers.xiaomuma.canal.factory;


import ch.qos.logback.classic.db.names.TableName;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.lang3.StringUtils;
import pers.xiaomuma.canal.constant.UndefinedTableName;
import pers.xiaomuma.canal.hander.EntryHandler;
import pers.xiaomuma.canal.utils.EntryUtils;
import pers.xiaomuma.canal.utils.FieldUtils;
import pers.xiaomuma.canal.utils.GenericUtils;
import pers.xiaomuma.canal.utils.HandlerUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class EntryColumnFactory extends AbstractColumnFactory<List<CanalEntry.Column>> {


    @Override
    public <R> R newInstance(Class<R> clazz, List<CanalEntry.Column> columns) throws Exception {
        R object = clazz.newInstance();
        Map<String, String> columnNames = EntryUtils.getFieldName(object.getClass());
        for (CanalEntry.Column column : columns) {
            String fieldName = columnNames.get(column.getName());
            if (StringUtils.isNotEmpty(fieldName)) {
                FieldUtils.setFieldValue(object, fieldName, column.getValue());
            }
        }
        return object;
    }

    @Override
    public <R> R newInstance(EntryHandler handler, List<CanalEntry.Column> columns) throws Exception {
        String canalTableName = HandlerUtils.getCanalTableName(handler);
        if (UndefinedTableName.ALL.name().toLowerCase().equals(canalTableName)) {
            Map<String, String> map = columns.stream().collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
            return (R) map;
        }
        Class<R> tableClass = GenericUtils.getTableClass(handler);
        if (tableClass != null) {
            return newInstance(tableClass, columns);
        }
        return null;
    }

    @Override
    public <R> R newInstance(EntryHandler handler, List<CanalEntry.Column> columns, Set<String> updateColumn) throws Exception {
        String canalTableName = HandlerUtils.getCanalTableName(handler);
        if (UndefinedTableName.ALL.name().toLowerCase().equals(canalTableName)) {
            Map<String, String> map = columns.stream().filter(column -> updateColumn.contains(column.getName()))
                    .collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
            return (R) map;
        }
        Class<R> tableClass = GenericUtils.getTableClass(handler);
        if (tableClass != null) {
            R object = tableClass.newInstance();
            Map<String, String> columnNames = EntryUtils.getFieldName(object.getClass());
            for (CanalEntry.Column column : columns) {
                if (updateColumn.contains(column.getName())) {
                    String fieldName = columnNames.get(column.getName());
                    if (StringUtils.isNotEmpty(fieldName)) {
                        FieldUtils.setFieldValue(object, fieldName, column.getValue());
                    }
                }
            }
            return object;
        }
        return null;
    }
}
