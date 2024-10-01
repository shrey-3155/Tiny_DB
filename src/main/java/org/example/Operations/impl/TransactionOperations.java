package org.example.Operations.impl;

import org.example.Operations.TransactionOperationsInterface;
import org.example.Query.impl.DDLQueryForDatabase;
import org.example.Query.impl.DDLQueryForTable;
import org.example.Utils.impl.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionOperations implements TransactionOperationsInterface {

    private boolean isTransactionStarted = false;
    public final List<String> transactionLog = new ArrayList<>();
    private final DDLQueryForDatabase ddlQueryForDatabase;
    public final Map<String, Map<String, List<String>>> allTableData = new HashMap<>();
    public final Map<String, Map<String, List<String>>> rollbackMap = new HashMap<>();

    public TransactionOperations(DDLQueryForDatabase ddlQueryForDatabase) {
        this.ddlQueryForDatabase = ddlQueryForDatabase;
    }

    @Override
    public boolean isTransactionStarted() {
        return isTransactionStarted;
    }

    @Override
    public void startTransaction() throws IOException {
        if (isTransactionStarted) {
            commitTransaction();
        }
        isTransactionStarted = true;
        transactionLog.clear();
        try {
            loadAllTablesData();
            if (allTableData.isEmpty()) {
                Logger.logEvent("No tables found to load data.");
            } else {
                Logger.logEvent("Transaction started successfully.");
            }
        } catch (IOException e) {
            Logger.logEvent("Error while starting transaction: " + e.getMessage());
            throw e;
        }
    }

    public void loadAllTablesData() throws IOException {
        File dbDir = new File(ddlQueryForDatabase.DB_PATH_NAME);
        File[] tableFiles = dbDir.listFiles((dir, name) -> name.endsWith(".txt") && !name.endsWith("_metadata.txt"));
        if (tableFiles != null) {
            if (tableFiles.length == 0) {
                Logger.logEvent("No table files found in directory.");
            }
            for (File tableFile : tableFiles) {
                String tableName = tableFile.getName().replace(".txt", "");
                Map<String, List<String>> tableData = new HashMap<>();
                try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
                    String headerLine = br.readLine();
                    if (headerLine != null) {
                        String[] columns = headerLine.split("\\|");
                        for (String column : columns) {
                            tableData.put(column.trim(), new ArrayList<>());
                        }

                        String dataLine;
                        while ((dataLine = br.readLine()) != null) {
                            String[] values = dataLine.split("\\|");
                            for (int i = 0; i < values.length; i++) {
                                tableData.get(columns[i].trim()).add(values[i].trim());
                            }
                        }
//                        if (tableData.values().stream().allMatch(List::isEmpty)) {
//                            Logger.logEvent("Table " + tableName + " is empty.");
//                        }
                    } else {
                        Logger.logEvent("Header line is missing in table file " + tableFile.getName());
                    }
                } catch (IOException e) {
                    Logger.logEvent("Error while loading table data for " + tableName + ": " + e.getMessage());
                    throw e;
                }
                allTableData.put(tableName, tableData);
            }
        }
    }

    @Override
    public void rollbackTransaction() throws IOException {
        isTransactionStarted = false;
        allTableData.clear();
        try {
            loadAllTablesData();
                writeAllTablesData(allTableData);
                Logger.logEvent("Transaction rolled back successfully.");

        } catch (IOException e) {
            Logger.logEvent("Error while rolling back transaction: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void executeBufferedOperations() throws IOException {
        try {
            if (transactionLog.isEmpty()) {
                Logger.logEvent("No operations to execute.");
                return;
            }
            for (String operation : transactionLog) {
                System.out.println("Executing: " + operation);
                Logger.logQuery(operation); // Log each operation being executed
            }
            commitTransaction();
            Logger.logEvent("Buffered operations executed successfully.");
        } catch (IOException e) {
            Logger.logEvent("Error while executing buffered operations: " + e.getMessage());
            throw e;
        }
    }

    private void commitTransaction() throws IOException {
        try {
            writeAllTablesData(allTableData);
            isTransactionStarted = false;
            allTableData.clear();
            Logger.logEvent("Transaction committed successfully.");
        } catch (IOException e) {
            Logger.logEvent("Error while committing transaction: " + e.getMessage());
            throw e;
        }
    }

    private void writeAllTablesData(Map<String, Map<String, List<String>>> dataToWrite) throws IOException {
        if (dataToWrite.isEmpty()) {
            Logger.logEvent("No data to write to tables.");
        }
        for (Map.Entry<String, Map<String, List<String>>> tableEntry : dataToWrite.entrySet()) {
            String tableName = tableEntry.getKey();
            Map<String, List<String>> tableData = tableEntry.getValue();

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(ddlQueryForDatabase.DB_PATH_NAME + "/" + tableName + ".txt"))) {
                // Write header
                String header = String.join(" | ", tableData.keySet());
                writer.write(header);
                writer.newLine();

                // Write rows
                int numRows = tableData.values().iterator().next().size();
                for (int i = 0; i < numRows; i++) {
                    StringBuilder row = new StringBuilder();
                    for (String column : tableData.keySet()) {
                        // Trim single quotes from the value
                        String value = tableData.get(column).get(i).replaceAll("^'|'$", "");
                        row.append(value);
                        row.append(" | ");
                    }
                    // Remove the last " | "
                    row.setLength(row.length() - 3);
                    writer.write(row.toString());
                    writer.newLine();
                }
            } catch (IOException e) {
                Logger.logEvent("Error while writing data for table " + tableName + ": " + e.getMessage());
                throw e;
            }
        }
    }

}
