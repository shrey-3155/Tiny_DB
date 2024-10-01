package org.example.Query;

import java.io.IOException;
import java.util.Map;

public interface DDLQueryTableInterface {
    public void createTable(String path,String tableName, Map<String, String> columnDetails) throws IOException;
}
