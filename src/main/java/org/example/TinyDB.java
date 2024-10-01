package org.example;

import org.example.Authentication.impl.UserAuthentication;
import org.example.Operations.impl.QueryParserOperations;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.InputMismatchException;
import java.util.Scanner;

public class TinyDB {
    public static void main(String[] args) throws IOException {

        UserAuthentication userAuthentication = new UserAuthentication();

        String createDatabaseQuery = "CREATE DATABASE my_database;";
        String useDatabaseQuery = "USE my_database;";
        String createTableQuery = "CREATE TABLE my_table (id INT, name VARCHAR(255));";
        String insertQuery = "INSERT INTO my_table (id, name) VALUES (1, 'John');";
        String selectQuery = "SELECT * FROM my_table WHERE id = 1;";
        String updateQuery = "UPDATE my_table SET name = 'Mike' WHERE id = 1;";
        String deleteQuery = "DELETE FROM my_table WHERE id = 1;";
        String dropTableQuery = "DROP TABLE my_table;";
        String foreignKeyCreate = "CREATE TABLE test_table (id INT FOREIGN KEY REFERENCES my_table(id), name VARCHAR(255)) PRIMARY KEY);";

        Scanner scanner = new Scanner(System.in);
        boolean exit = false;

        while (!exit) {
            System.out.println("\nWelcome to TinyDB");
            System.out.println("1. Register");
            System.out.println("2. Login");
            System.out.println("3. Exit");
            System.out.print("Choose an option: ");


            try {
                int choice = scanner.nextInt();
                scanner.nextLine();
                switch (choice) {
                    case 1:
                        userAuthentication.register();
                        break;
                    case 2:
                        if (userAuthentication.login()) {
                            userMenu();
                        }
                        break;
                    case 3:
                        exit = true;
                        break;
                    default:
                        System.out.println("Invalid choice, please try again.");
                }
            } catch (InputMismatchException | NoSuchAlgorithmException e) {
                System.out.println("Please enter valid input between 1 to 3! ");
                scanner.nextLine(); // Consume the invalid input

            }
        }
    }
    private static void userMenu() throws IOException {
        Scanner scanner = new Scanner(System.in);
        boolean exit = false;
        QueryParserOperations qp = new QueryParserOperations();
        while (!exit) {
            System.out.println("\nUser Menu");
            System.out.println("1. Write Queries");
            System.out.println("2. Export Data and Structure");
            System.out.println("3. ERD");
            System.out.println("4. Exit");
            System.out.print("Choose an option: ");
            try {
                int choice = scanner.nextInt();
                scanner.nextLine(); // Consume newline

                switch (choice) {
                    case 1:
                        Scanner query = new Scanner(System.in);
                        while (true) {
                            System.out.print("Enter your query: ");
                            String command = query.nextLine().trim();
                            if (command.equalsIgnoreCase("exit")) {
                                System.out.println("Exiting TinyDB Query mode.");
                                break;
                            }
                            System.out.println("Writing Queries...");
                            qp.parseQuery(command);
                        }
                        break;
                    case 2:
                        System.out.println("Exporting Data and Structure...");
                        qp.exportStructure();
                        break;
                    case 3:
                        Scanner query1 = new Scanner(System.in);
                        while (true) {
                            System.out.print("Enter your database name: ");
                            String command = query1.nextLine().trim();
                            if (command.equalsIgnoreCase("exit")) {
                                System.out.println("Exiting TinyDB Query mode.");
                                break;
                            }
                            qp.createERD(command);
                        }
                        break;
                    case 4:
                        exit = true;
                        System.out.println("Exiting User Menu. Goodbye!");
                        break;
                    default:
                        System.out.println("Invalid choice. Please enter a number between 1 and 4.");
                }
            } catch (InputMismatchException e) {
                System.out.println("Invalid input. Please enter a number.");
                scanner.nextLine(); // Consume the invalid input
            }

        }
    }
}