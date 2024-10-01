package org.example.Operations.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.example.Operations.TableOperationsInterface;
import org.example.Query.impl.DDLQueryForDatabase;
import org.example.Query.impl.DDLQueryForTable;
import org.example.Query.impl.DMLQuery;
import org.example.Utils.impl.Logger;

public class TableOperations implements TableOperationsInterface {
    private final DDLQueryForDatabase ddlQueryForDatabase;
    private final TransactionOperations transactionOperations;
    private final TransactionManager transactionManager;

    DDLQueryForTable ddlQueryForTable = new DDLQueryForTable();
    DMLQuery dmlQuery = new DMLQuery();

    public TableOperations(DDLQueryForDatabase ddlQueryForDatabase, TransactionOperations transactionOperations, TransactionManager transactionManager) {
        this.ddlQueryForDatabase = ddlQueryForDatabase;
        this.transactionOperations = transactionOperations;
        this.transactionManager = transactionManager;
    }

    @Override
    public void createTable(String tableName, String columnDetails) throws IOException {

        if (ddlQueryForDatabase.DB_PATH_NAME == null) {
            System.out.println("No Database selected!");
            return;
        }

        try {
            // Remove the "Columns: " prefix and trim any leading or trailing spaces
            String columnsPart = columnDetails.replaceFirst("Columns: ", "").trim();

            // Split the column definitions by comma
            String[] columns = columnsPart.split(",");
            System.out.println(Arrays.toString(columns));

            Map<String, String> columnList = new HashMap<>();
            for (String column : columns) {
                // Split each column definition by spaces
                String[] parts = column.trim().split("\\s+");

                if (parts.length < 2) {
                    throw new IllegalArgumentException("Invalid column definition: " + column);
                }

                String columnName = parts[0];
                String columnType = parts[1];
                String relationship = "None"; // Default relationship

                // Check for relationships (e.g., FOREIGN KEY, PRIMARY KEY, etc.)
                if (parts.length > 2) {
                    StringBuilder relationshipBuilder = new StringBuilder();
                    for (int i = 2; i < parts.length; i++) {
                        if (i > 2) {
                            relationshipBuilder.append(" ");
                        }
                        relationshipBuilder.append(parts[i]);
                    }
                    relationship = relationshipBuilder.toString();

                    // Special handling for FOREIGN KEY to include the referenced table
                    if (relationship.contains("FOREIGN KEY")) {
                        String[] relationshipParts = relationship.split("REFERENCES");
                        if (relationshipParts.length == 2) {
                            String referencedTable = relationshipParts[1].trim().replaceAll("[(),]", "");
                            relationship = "FOREIGN KEY REFERENCES " + referencedTable;
                        }
                    }
                }
                columnList.put(columnName, columnType + " | " + relationship);
            }

            ddlQueryForTable.createTable(ddlQueryForDatabase.DB_PATH_NAME, tableName, columnList);
        } catch (Exception e) {
           e.printStackTrace();
        }
    }

    @Override
    public void insertRow(String tableName, String colName, String values) throws IOException {
        String query = "INSERT INTO " + tableName + " (" + colName + ") VALUES (" + values + ")";

        if (transactionOperations.isTransactionStarted()) {
            transactionOperations.transactionLog.add(query);
            transactionOperations.loadAllTablesData();
            transactionManager.insertRow(tableName, colName, values);
            return;
        }

        // Split the column names
        String[] columns = colName.split(",\\s*");
        // Process the values string to handle quoted strings properly
        String[] valueArray = splitValues(values);

        // Validate that the number of columns matches the number of values
        if (columns.length != valueArray.length) {
            System.out.println("Error: Number of columns and values must match.");
        }

        // Check if the table exists
        File tableFile = new File(ddlQueryForDatabase.DB_PATH_NAME + File.separator + tableName + ".txt");
        if (!tableFile.exists()) {
            System.out.println("Error: Table does not exist: " + tableFile);
            Logger.logEvent("Error: Table does not exist: " + tableFile);
            return;
        }

        // Load table schema to check if columns exist
        Map<String, Integer> tableSchema = dmlQuery.getTableSchema(ddlQueryForDatabase.DB_PATH_NAME, tableName);
        for (String column : columns) {
            if (!tableSchema.containsKey(column.trim())) {
                System.out.println("Error: Column does not exist: " + column.trim());            }
        }

        Map<String, String> rowData = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            String cleanedValue = valueArray[i].trim().replaceAll("^'|'$", ""); // Remove leading and trailing single quotes
            rowData.put(columns[i].trim(), cleanedValue);
        }

        dmlQuery.insertIntoRows(ddlQueryForDatabase.DB_PATH_NAME, tableName, rowData);
    }

    @Override
    public void selectFromTable(String tableName, String columns, String whereCondition) throws IOException {
        // Log the query
        String query = "SELECT " + columns + " FROM " + tableName + (whereCondition.equals("none") ? "" : " WHERE " + whereCondition);

        if (transactionOperations.isTransactionStarted()) {
            transactionOperations.transactionLog.add(query);
            transactionOperations.loadAllTablesData();
            transactionManager.selectFromTable(tableName, columns, whereCondition);
            Logger.logEvent("Transaction started for select operation on table '" + tableName + "'");
            return;
        }

        try {
            // Split the column names
            String[] columnSeparated = columns.split(",\\s*");

            if (whereCondition.equals("none")) {
                dmlQuery.selectFromTable(ddlQueryForDatabase.DB_PATH_NAME, tableName, columnSeparated);
            } else {
                dmlQuery.selectFromTableCondition(ddlQueryForDatabase.DB_PATH_NAME, tableName, columnSeparated, whereCondition);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void updateToTable(String tableName, String colValuePair, String condition) throws IOException {
        String query = "UPDATE " + tableName + " SET " + colValuePair + " WHERE " + condition;

        if (transactionOperations.isTransactionStarted()) {
            transactionOperations.transactionLog.add(query);
            transactionOperations.loadAllTablesData();
            transactionManager.updateToTable(tableName, colValuePair, condition);
            return;
        }

            dmlQuery.updateToTable(ddlQueryForDatabase.DB_PATH_NAME, tableName, colValuePair, condition);

    }

    @Override
    public void deleteRow(String tableName, String condition) throws IOException {
        if (condition == null || condition.trim().isEmpty()) {
            throw new IllegalArgumentException("WHERE condition cannot be null or empty.");
        }

        if (!condition.contains("=")) {
            throw new IllegalArgumentException("Invalid WHERE condition. Must be in the format 'column=value'.");
        }
        String query = "DELETE FROM " + tableName + " WHERE " + condition;

        if (transactionOperations.isTransactionStarted()) {
            transactionOperations.transactionLog.add(query);
            transactionOperations.loadAllTablesData();
            transactionManager.deleteRow(tableName, condition);
            return;
        }

            dmlQuery.deleteFromTable(ddlQueryForDatabase.DB_PATH_NAME, tableName, condition);
    }

    private String[] splitValues(String values) {
        return values.split(",(?=([^']*'[^']*')*[^']*$)");
    }

}
