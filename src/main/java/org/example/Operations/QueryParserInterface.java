package org.example.Operations;

import java.io.IOException;

public interface QueryParserInterface {
    public void parseQuery(String query) throws IOException;
    public void parseSelectQuery(String query);
    public void parseCreateQuery(String query);
    public void parseUpdateQuery(String query);
    public void parseDeleteQuery(String query);
    public void parseInsertQuery(String query);
    public void parseDropQuery(String query);
    public void createERD(String databaseName) throws IOException;

}
