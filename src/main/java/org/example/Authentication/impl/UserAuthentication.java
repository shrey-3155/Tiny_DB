package org.example.Authentication.impl;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;

public class UserAuthentication {
    private static final String USER_FILE = "src/main/java/org/example/UserInfo/User_Profile.txt";

    // Method to hash a given input using SHA-256
    private static String hash(String input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = md.digest(input.getBytes());
        StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    // Method to check if a userID already exists in the file
    private static boolean isUserIDExists(String hashedUserID, String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Split the line by the pipe symbol |
                String[] parts = line.split("\\|"); // The backslashes are to escape the pipe character in regex

                // Check if the first part (hashedUserID) matches
                if (parts[0].equals(hashedUserID)) {
                    return true; // UserID exists
                }
            }
        }
        return false; // UserID does not exist
    }


    // Method to register a new user
    public static void register() throws NoSuchAlgorithmException, IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter UserID: ");
        String userID = scanner.nextLine();
        String hashedUserID = hash(userID);
        // Check if the userID already exists
        if (isUserIDExists(hashedUserID, USER_FILE)) {
            System.out.println("UserID already exists. Please choose a different UserID.");
            return;
        }
        System.out.print("Enter Password: ");
        String password = scanner.nextLine();
        System.out.print("Enter Security Question: ");
        String securityQuestion = scanner.nextLine();
        System.out.print("Enter Security Answer: ");
        String securityAnswer = scanner.nextLine();




        String hashedPassword = hash(password);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(USER_FILE, true))) {
            writer.write(hashedUserID + "|" + hashedPassword + "|" + securityQuestion + "|" + securityAnswer);
            writer.newLine();
        }

        System.out.println("Registration successful!");
    }

    // Method to login a user
    public static boolean login() throws NoSuchAlgorithmException, IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter UserID: ");
        String userID = scanner.nextLine();
        String hashedUserID = hash(userID);
        if (!isUserIDExists(hashedUserID, USER_FILE)) {
            System.out.println("UserID does not exists! Please choose a different UserID.");
            return false;
        }
        System.out.print("Enter Password: ");
        String password = scanner.nextLine();

        String hashedPassword = hash(password);

        try (BufferedReader reader = new BufferedReader(new FileReader(USER_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts[0].equals(hashedUserID) && parts[1].equals(hashedPassword)) {
                    System.out.print("Answer Security Question: " + parts[2] + " ");
                    String securityAnswer = scanner.nextLine();
                    if (parts[3].equals(securityAnswer)) {
                        System.out.println("Login successful!");
                        return true;
                    } else {
                        System.out.println("Security answer incorrect.");
                        return false;
                    }
                }
            }
        }

        System.out.println("UserID or Password incorrect.");
        return false;
    }
}
