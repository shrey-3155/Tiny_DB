package org.example.Query;

import java.io.IOException;
import java.util.Map;

public interface DMLQueryInterface {
    public void insertIntoRows(String path, String columnName, Map<String, String> rowValues) throws IOException;

    public void deleteFromTable(String path, String tableName, String whereCondition) throws IOException;

    void selectFromTable(String path, String tableName, String[] columns) throws IOException;
    public void updateToTable(String path, String tableName, String colValuePair, String condition) throws IOException ;

}
