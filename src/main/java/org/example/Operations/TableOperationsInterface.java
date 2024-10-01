package org.example.Operations;

import java.io.IOException;

public interface TableOperationsInterface {
    public void createTable(String tableName, String columnDetails) throws IOException;
    public void insertRow(String tableName, String colName, String values) throws IOException;
    public void deleteRow(String tableName, String condition) throws IOException;
    public void selectFromTable(String tableName, String columns, String whereCondtion) throws IOException;
    public void updateToTable(String tableName, String colValuePair, String condition) throws IOException;
    }
