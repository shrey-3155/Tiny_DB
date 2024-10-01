package org.example.Operations.impl;

import org.example.Query.impl.DDLQueryForDatabase;
import org.example.Utils.impl.SQLDumpUtility;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParserOperations implements org.example.Operations.QueryParserInterface {
    DDLQueryForDatabase ddlQueryForDatabase = new DDLQueryForDatabase();
    SQLDumpUtility utility = new SQLDumpUtility();
    TransactionOperations transactionOperations = new TransactionOperations(ddlQueryForDatabase);
    TransactionManager transactionManager = new TransactionManager(transactionOperations);
    TableOperations tableOperations = new TableOperations(ddlQueryForDatabase,transactionOperations,transactionManager);
    DatabaseOperations databaseOperations = new DatabaseOperations(ddlQueryForDatabase,utility,transactionOperations);
    @Override
    public void parseSelectQuery(String sqlQuery) {
        // Define the regex pattern
        String pattern1 = "SELECT\\s+(.*?)\\s+FROM\\s+(\\w+);";
        String pattern2 = "SELECT\\s+(.*?)\\s+FROM\\s+(\\w+)\\s+WHERE\\s+(\\w+\\s*=\\s*\\d+);";

        // Create a Pattern object
        Pattern p1 = Pattern.compile(pattern1, Pattern.CASE_INSENSITIVE);
        Pattern p2 = Pattern.compile(pattern2, Pattern.CASE_INSENSITIVE);

        // Create a Matcher object
        Matcher m1 = p1.matcher(sqlQuery);
        Matcher m2 = p2.matcher(sqlQuery);

        // Check if a match is found
        if (m1.find()) {
            // Extract the matched groups
            String selectClause = m1.group(1).trim();
            String fromClause = m1.group(2).trim();
            //String whereClause = m1.group(3).trim();

            // Print the parsed components
            System.out.println("SELECT clause: " + selectClause);
            System.out.println("FROM clause: " + fromClause);
            //System.out.println("WHERE clause: " + whereClause);
        } else if(m2.find()){
            String selectClause = m2.group(1).trim();
            String fromClause = m2.group(2).trim();
            String whereClause = m2.group(3).trim();

            // Print the parsed components
            System.out.println("SELECT clause: " + selectClause);
            System.out.println("FROM clause: " + fromClause);
            System.out.println("WHERE clause: " + whereClause);

        }
        else {
            System.out.println("No match found.");
        }
    }

    @Override
    public void parseCreateQuery(String sqlQuery) {}

    @Override
    public void parseUpdateQuery(String sqlQuery) {}

    @Override
    public void parseDeleteQuery(String query) {

    }

    @Override
    public void parseInsertQuery(String query) {

    }

    @Override
    public void parseDropQuery(String query) {

    }

    @Override
    public void parseQuery(String sqlQuery) throws IOException {
        // Define regex patterns for each SQL command
        String createDatabasePattern = "CREATE\\s+DATABASE\\s+(\\w+);";
        String useDatabasePattern = "USE\\s+(\\w+);";
        String createTablePattern = "CREATE\\s+TABLE\\s+(\\w+)\\s*\\((.*?)\\);";
        String insertPattern = "INSERT\\s+INTO\\s+(\\w+)\\s*\\((.*?)\\)\\s*VALUES\\s*\\((.*?)\\);";
        String selectPattern = "SELECT\\s+([^;]+)\\s+FROM\\s+(\\w+)\\s+WHERE\\s+(\\w+\\s*(?:=|<|>|!=|<=|>=)\\s*(?:'\\w+'|\\d+));";
        String selectAllTablePattern = "SELECT\\s+(.*?)\\s+FROM\\s+(\\w+);";
        String updatePattern = "UPDATE\\s+(\\w+)\\s+SET\\s+([^;]+)\\s+WHERE\\s+([^;]+);";
        String deletePattern = "DELETE\\s+FROM\\s+(\\w+)\\s+WHERE\\s+(\\w+\\s*=\\s*\\d+);";
        String dropTablePattern = "DROP\\s+TABLE\\s+(\\w+);";
        String startTransaction = "(?i)\\bstart\\s+transaction\\s*;";
        String commitTransaction = "(?i)\\bcommit\\s*;";
        String rollbackTransaction = "(?i)\\brollback\\s*;";

        // Create Pattern objects for each regex pattern
        Pattern createDatabaseP = Pattern.compile(createDatabasePattern, Pattern.CASE_INSENSITIVE);
        Pattern useDatabaseP = Pattern.compile(useDatabasePattern, Pattern.CASE_INSENSITIVE);
        Pattern createTableP = Pattern.compile(createTablePattern, Pattern.CASE_INSENSITIVE);
        Pattern insertP = Pattern.compile(insertPattern, Pattern.CASE_INSENSITIVE);
        Pattern selectP = Pattern.compile(selectPattern, Pattern.CASE_INSENSITIVE);
        Pattern selectAllTableP = Pattern.compile(selectAllTablePattern, Pattern.CASE_INSENSITIVE);
        Pattern updateP = Pattern.compile(updatePattern, Pattern.CASE_INSENSITIVE);
        Pattern deleteP = Pattern.compile(deletePattern, Pattern.CASE_INSENSITIVE);
        Pattern dropTableP = Pattern.compile(dropTablePattern, Pattern.CASE_INSENSITIVE);
        Pattern startTrans = Pattern.compile(startTransaction,Pattern.CASE_INSENSITIVE);
        Pattern commitTrans = Pattern.compile(commitTransaction,Pattern.CASE_INSENSITIVE);
        Pattern rollbackTrans = Pattern.compile(rollbackTransaction,Pattern.CASE_INSENSITIVE);

        // Matcher object to match each pattern against the SQL query
        Matcher matcher;

        // Check which SQL command the query matches and parse accordingly
        if ((matcher = createDatabaseP.matcher(sqlQuery)).matches()) {
            String dbName = matcher.group(1);
            databaseOperations.createDatabase(dbName);
        } else if ((matcher = useDatabaseP.matcher(sqlQuery)).matches()) {
            String dbName = matcher.group(1);
            databaseOperations.useDatabase(dbName);
        } else if ((matcher = createTableP.matcher(sqlQuery)).matches()) {
            if(ddlQueryForDatabase.DB_PATH_NAME == null){
                System.out.println("Database not selected");
                return;
            }
            String tableName = matcher.group(1);
            String columns = matcher.group(2);
            tableOperations.createTable(tableName,columns);
        } else if ((matcher = insertP.matcher(sqlQuery)).matches()) {
            if(ddlQueryForDatabase.DB_PATH_NAME == null){
                System.out.println("Database not selected");
                return;
            }
            String tableName = matcher.group(1);
            String columns = matcher.group(2);
            String values = matcher.group(3);
            tableOperations.insertRow(tableName,columns,values);
        }else if ((matcher = selectAllTableP.matcher(sqlQuery)).matches()) {
            if(ddlQueryForDatabase.DB_PATH_NAME == null){
                System.out.println("Database not selected");
                return;
            }
            String columns = matcher.group(1);
            String tableName = matcher.group(2);
            tableOperations.selectFromTable(tableName,columns,"none");
        }else if ((matcher = selectP.matcher(sqlQuery)).matches()) {
            if(ddlQueryForDatabase.DB_PATH_NAME == null){
                System.out.println("Database not selected");
                return;
            }
            String columns = matcher.group(1);
            String tableName = matcher.group(2);
            String condition = matcher.group(3).trim();
            tableOperations.selectFromTable(tableName,columns,condition);
        } else if ((matcher = updateP.matcher(sqlQuery)).matches()) {
            if(ddlQueryForDatabase.DB_PATH_NAME == null){
                System.out.println("Database not selected");
                return;
            }
            String tableName = matcher.group(1);
            String columnValuePairs = matcher.group(2);
            String condition = matcher.group(3).trim();
            tableOperations.updateToTable(tableName,columnValuePairs,condition);
        } else if ((matcher = deleteP.matcher(sqlQuery)).matches()) {
            if(ddlQueryForDatabase.DB_PATH_NAME == null){
                System.out.println("Database not selected");
                return;
            }
            String tableName = matcher.group(1);
            String condition = matcher.group(2).trim();
            tableOperations.deleteRow(tableName,condition);
        } else if ((matcher = dropTableP.matcher(sqlQuery)).matches()) {
            if(ddlQueryForDatabase.DB_PATH_NAME == null){
                System.out.println("Database not selected");
                return;
            }
            String tableName = matcher.group(1);
            databaseOperations.dropTable(tableName);
        } else if ((matcher = startTrans.matcher(sqlQuery)).matches()) {
            if(ddlQueryForDatabase.DB_PATH_NAME == null){
                System.out.println("Database not selected");
                return;
            }
            transactionOperations.startTransaction();
        }
        else if ((matcher = commitTrans.matcher(sqlQuery)).matches()) {
            if(ddlQueryForDatabase.DB_PATH_NAME == null){
                System.out.println("Database not selected");
                return;
            }
            transactionOperations.executeBufferedOperations();
        }
        else if ((matcher = rollbackTrans.matcher(sqlQuery)).matches()) {
            if(ddlQueryForDatabase.DB_PATH_NAME == null){
                System.out.println("Database not selected");
                return;
            }
            transactionOperations.rollbackTransaction();
        }else {
            System.out.println("Invalid SQL Query");
        }
    }

    @Override
    public void createERD(String databaseName) throws IOException {
        String DB_PATH_NAME = "src/main/java/org/example/Database/"+databaseName;
        File databaseFile = new File(DB_PATH_NAME);
        if(!databaseFile.exists()){
            System.out.println("Database does not exist");
            return;
        }
        System.out.println("Generating ERD...");
        databaseOperations.createERD(databaseName);
    }

    public void exportStructure() throws IOException {
        Scanner sc = new Scanner(System.in);
        File databaseFolder = new File("src/main/java/org/example/Database");
        File[] dbs = databaseFolder.listFiles();
        System.out.println("Choose option from below\n");
        for(int i = 0; i < dbs.length; i++){
            System.out.println(+i+1+": "+dbs[i].getName());
        }
        System.out.print("Choice: ");
        int choice = sc.nextInt() - 1;
        if(choice < 0 || choice >= dbs.length){
            System.out.println("Invalid choice");
        }
        else{
            databaseOperations.generateSqlDump(dbs[choice].getAbsolutePath());
        }
        //databaseOperations.generateSqlDump(dbs[choice].getAbsolutePath());
    }
}
