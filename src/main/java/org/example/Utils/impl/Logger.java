package org.example.Utils.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {
    private static String queryLogPath;
    private static String generalLogPath;
    private static String eventLogPath;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static synchronized void initializeLogger(String databaseName) throws IOException {
        // Define the path where the database should be located
        File databaseDir = new File("src/main/java/org/example/Database/" + databaseName);

        // Check if the database directory exists
        if (!databaseDir.exists()) {
            throw new IOException("Database does not exist: " + databaseDir.getPath());
        }

        // Create the directory for the database logs if it doesn't exist
        File logDir = new File("src/main/java/org/example/Logs/" + databaseName);
        if (!logDir.exists()) {
            if (!logDir.mkdirs()) {
                throw new IOException("Failed to create log directory.");
            }
        }

        // Set log file paths
        queryLogPath = logDir.getPath() + "/query_log.txt";
        generalLogPath = logDir.getPath() + "/general_log.txt";
        eventLogPath = logDir.getPath() + "/event_log.txt";

        // Create empty log files if they do not exist
        createFileIfNotExists(queryLogPath);
        createFileIfNotExists(generalLogPath);
        createFileIfNotExists(eventLogPath);
    }

    private static void createFileIfNotExists(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            // Ensure the parent directory exists
            File parentDir = file.getParentFile();
            if (!parentDir.exists() && !parentDir.mkdirs()) {
                throw new IOException("Failed to create parent directory.");
            }
            if (!file.createNewFile()) {
                throw new IOException("Failed to create log file: " + filePath);
            }
        }
    }

    public static synchronized void logQuery(String query) {
        String timestamp = LocalDateTime.now().format(formatter);
        String logEntry = String.format("Timestamp: %s, Query: %s", timestamp, query);
        try (FileWriter fw = new FileWriter(queryLogPath, true)) {
            fw.write(logEntry + "\n");
        } catch (IOException e) {
            System.err.println("Failed to write to query log: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static synchronized void logGeneral(String executionTime, int tableCount, int recordCount) {
        String timestamp = LocalDateTime.now().format(formatter);
        String logEntry = String.format("Timestamp: %s, Execution time: %s ms, Table count: %d, Record count: %d", timestamp, executionTime, tableCount, recordCount);
        try (FileWriter fw = new FileWriter(generalLogPath, true)) {
            fw.write(logEntry + "\n");
        } catch (IOException e) {
            System.err.println("Failed to write to general log: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static synchronized void logEvent(String eventDescription) {
        String timestamp = LocalDateTime.now().format(formatter);
        String logEntry = String.format("Timestamp: %s, Event: %s", timestamp, eventDescription);
        try (FileWriter fw = new FileWriter(eventLogPath, true)) {
            fw.write(logEntry + "\n");
        } catch (IOException e) {
            System.err.println("Failed to write to event log: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
