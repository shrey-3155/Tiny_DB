package org.example.Operations.impl;

import org.example.Operations.DatabaseOperationInterface;
import org.example.Query.impl.DDLQueryForDatabase;
import org.example.Utils.impl.Logger;
import org.example.Utils.impl.SQLDumpUtility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class DatabaseOperations implements DatabaseOperationInterface {
    private final DDLQueryForDatabase ddlQueryForDatabase;
    private final SQLDumpUtility sqlDumpUtilityutility;
    private final TransactionOperations transactionOperations;

    public DatabaseOperations(DDLQueryForDatabase ddlQueryForDatabase, SQLDumpUtility utility, TransactionOperations transactionOperations) {
        this.ddlQueryForDatabase = ddlQueryForDatabase;
        this.sqlDumpUtilityutility = utility;
        this.transactionOperations = transactionOperations;
    }

    @Override
    public void createDatabase(String databaseName) throws IOException {

            ddlQueryForDatabase.createDatabase(databaseName);
    }

    @Override
    public void useDatabase(String databaseName) throws IOException {

            ddlQueryForDatabase.useDatabase(databaseName);

    }

    @Override
    public void dropTable(String tableName) throws IOException {
        if (ddlQueryForDatabase.DB_PATH_NAME == null) {
            System.out.println("No Database selected!");
            return;
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty.");
        }

            ddlQueryForDatabase.dropTable(ddlQueryForDatabase.DB_PATH_NAME, tableName);
    }

    public void generateSqlDump(String databaseFullPath) throws IOException {

            sqlDumpUtilityutility.export(databaseFullPath);
    }

    @Override
    public void createERD(String databaseName) throws IOException {
            File databaseDir = new File("src/main/java/org/example/ERD");
            File allERDFiles = new File("src/main/java/org/example/Database/" + databaseName);
            if (!databaseDir.exists()) {
                databaseDir.mkdirs();
            }
            File erdFile = new File(databaseDir, databaseName + "_ERD.txt");

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(erdFile))) {
                writer.write("Entity-Relationship Diagram (ERD) for Database: " + databaseName);
                writer.newLine();
                writer.write("=================================================");
                writer.newLine();

                // List all metadata files
                File[] files = allERDFiles.listFiles((dir, name) -> name.endsWith("_metadata.txt"));
                if (files != null) {
                    for (File metadataFile : files) {
                        String tableName = metadataFile.getName().replace("_metadata.txt", "");
                        writer.newLine();
                        writer.write("Table: " + tableName);
                        writer.newLine();
                        writer.write("Columns | Datatype | Constraints | FOREIGN KEY Table | Relationship");
                        writer.newLine();
                        writer.write("-------------------------------------------------");
                        writer.newLine();

                        // Read each metadata file
                        List<String> lines = Files.readAllLines(metadataFile.toPath());
                        for (String line : lines) {
                            writer.write(formatColumnDetails(line));
                            writer.newLine();
                        }

                        writer.write("-------------------------------------------------");
                        writer.newLine();
                    }
                }

                writer.write("ERD generation completed successfully.");
                System.out.println("ERD file created");
            }

    }

    // Helper method to format column details properly
    private String formatColumnDetails(String line) {
        String[] parts = line.split("\\|");
        if (parts.length >= 3) {
            String columnName = parts[0].trim();
            String columnType = parts[1].trim();
            String constraints = parts[2].trim();
            String foreignKeyRef = "NONE";
            String relationship = parts.length > 3 ? parts[3].trim() : "NONE";

            // Check if it's a foreign key and has a reference
            if (constraints.startsWith("FOREIGN KEY")) {
                foreignKeyRef = extractForeignKeyReference(constraints);
                constraints = "FOREIGN KEY"; // Simplify constraint to just "FOREIGN KEY"
            }

            return columnName + " | " + columnType + " | " + constraints + " | " + foreignKeyRef + " | " + relationship;
        }
        return line + " | - | - | - | -"; // Default for malformed lines
    }

    // Helper method to extract and format foreign key references
    private String extractForeignKeyReference(String constraint) {
        if (constraint.contains("REFERENCES")) {
            String[] parts = constraint.split("REFERENCES");
            if (parts.length == 2) {
                return parts[1].trim();
            }
        }
        return "NONE";
    }
}
