package org.example.Operations;

import java.io.IOException;

public interface DatabaseOperationInterface {
    public void createDatabase(String databaseName) throws IOException;
    public void useDatabase(String databaseName) throws IOException;
    public void dropTable(String tableName) throws IOException;
    public void generateSqlDump(String databaseName) throws IOException;
    public void createERD(String databaseName) throws IOException;
}
