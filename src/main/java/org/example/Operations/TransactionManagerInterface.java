package org.example.Operations;

import java.io.IOException;

public interface TransactionManagerInterface {
    void insertRow(String tableName, String colName, String values) throws IOException;

    void deleteRow(String tableName, String condition) throws IOException;

    void updateToTable(String tableName, String colValuePair, String condition) throws IOException;

    void selectFromTable(String tableName, String columns, String whereCondition) throws IOException;
}
