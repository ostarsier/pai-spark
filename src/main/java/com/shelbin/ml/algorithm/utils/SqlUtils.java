package com.shelbin.ml.algorithm.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlUtils {

    private static Map<String, String> escapeMap = new HashMap<>();

    static {
        escapeMap.put("mysql", "`");
        escapeMap.put("oracle", "\"");
        escapeMap.put("postgresql", "\"");
        escapeMap.put("sqlserver", "[");
        escapeMap.put("sqlite", "`");
    }

    public static String escapeStr(String jdbcUrl) {
        String database = getDatabaseFromUrl(jdbcUrl);
        return escapeMap.get(database);
    }

    public static String getDatabaseFromUrl(String jdbcUrl) {
        Pattern pattern = Pattern.compile("jdbc:(.*?):");
        Matcher matcher = pattern.matcher(jdbcUrl);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            throw new IllegalArgumentException("Failed to extract database name from jdbcUrl");
        }
    }

    public static void main(String[] args) {
        String jdbcUrl = "jdbc:postgresql://localhost:3306/mydatabase";
        String escapeString = escapeStr(jdbcUrl);
        System.out.println("Escape string for " + jdbcUrl + ": " + escapeString);
    }


}
