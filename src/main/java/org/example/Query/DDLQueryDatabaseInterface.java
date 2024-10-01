package org.example.Query;

import java.io.IOException;

public interface DDLQueryDatabaseInterface {
    public void createDatabase(String databaseName) throws IOException;
    public void useDatabase(String databaseName) throws IOException;
    void dropTable(String path, String tableName) throws IOException;

}
