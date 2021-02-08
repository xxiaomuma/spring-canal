package pers.xiaomuma.canal.utils;


import java.math.BigDecimal;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class StringConvertUtils {

    private static String[] PARSE_PATTERNS = {
            "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM",
            "yyyy/MM/dd", "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm", "yyyy/MM",
            "yyyy.MM.dd", "yyyy.MM.dd HH:mm:ss", "yyyy.MM.dd HH:mm", "yyyy.MM"};

    public static Object convertType(Class<?> type, String value) {
        if (value == null) {
            return null;
        } else if (type.equals(Integer.class)) {
            return Integer.parseInt(value);
        } else if (type.equals(Long.class)) {
            return Long.parseLong(value);
        } else if (type.equals(Boolean.class)) {
            return convertToBoolean(value);
        } else if (type.equals(BigDecimal.class)) {
            return new BigDecimal(value);
        } else if (type.equals(Double.class)) {
            return Double.parseDouble(value);
        } else if (type.equals(Float.class)) {
            return Float.parseFloat(value);
        } else if (type.equals(Date.class)) {
            return parseDate(value);
        } else if (type.equals(java.sql.Date.class)) {
            return parseDate(value);
        } else if (type.equals(LocalDateTime.class)) {
            return parseLocalDateTime(value);
        } else if (type.equals(LocalDate.class)) {
            return parseLocalDate(value);
        } else {
            return value;
        }
    }

    private static LocalDate parseLocalDate(String str) {
        return LocalDate.parse(str, DateTimeFormatter.ofPattern(PARSE_PATTERNS[0]));
    }

    private static LocalDateTime parseLocalDateTime(String str) {
        return LocalDateTime.parse(str, DateTimeFormatter.ofPattern(PARSE_PATTERNS[1]));
    }

    private static Date parseDate(String str) {
        if (str == null) {
            return null;
        }
        try {
            return org.apache.commons.lang.time.DateUtils.parseDate(str, PARSE_PATTERNS);
        } catch (ParseException e) {
            return null;
        }
    }

    private static boolean convertToBoolean(String value) {
        return "1".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value);
    }
}
