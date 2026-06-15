package Clients;

import models.Book;
import shared.Request;
import shared.Response;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Scanner;

public class BookClient {

    private final String host;
    private final int port;
    private String clientId;

    public BookClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.clientId = "CLIENT-" + System.currentTimeMillis();
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    private Response sendRequest(Request request) {
        try (Socket socket = new Socket(host, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeObject(request);
            out.flush();

            return (Response) in.readObject();

        } catch (Exception e) {
            return new Response(false, "Client error: " + e.getMessage(), null);
        }
    }

    // Helper: read a non-empty line
    private String readNonEmpty(Scanner scanner, String prompt) {
        String input;
        while (true) {
            System.out.print(prompt);
            input = scanner.nextLine().trim();
            if (!input.isEmpty()) {
                return input;
            }
            System.out.println("Input cannot be empty. Please try again.");
        }
    }

    // Helper: read a valid double
    private double readValidPrice(Scanner scanner, String prompt) {
        while (true) {
            System.out.print(prompt);
            String input = scanner.nextLine().trim();
            try {
                return Double.parseDouble(input);
            } catch (NumberFormatException e) {
                System.out.println("Invalid number. Please enter a valid price (e.g., 29.99).");
            }
        }
    }

    private void run() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n===== Book Client Menu =====");
            System.out.println("1. Add Book");
            System.out.println("2. Find Book by ID");
            System.out.println("3. Get All Books");
            System.out.println("4. Remove Book");
            System.out.println("0. Exit");
            System.out.print("Choose: ");

            String choice = scanner.nextLine().trim();

            if (choice.equals("0")) {
                System.out.println("Client closed.");
                break;
            }

            Response response = null;

            switch (choice) {
                case "1":
                    String id = readNonEmpty(scanner, "Enter Book ID: ");
                    String title = readNonEmpty(scanner, "Enter Title: ");
                    String author = readNonEmpty(scanner, "Enter Author: ");
                    double price = readValidPrice(scanner, "Enter Price: ");

                    Book newBook = new Book(id, title, author, price);
                    Request addRequest = new Request("ADD_BOOK", newBook);
                    addRequest.setClientId(clientId);
                    response = sendRequest(addRequest);
                    break;

                case "2":
                    String searchId = readNonEmpty(scanner, "Enter Book ID: ");
                    Request findRequest = new Request("FIND_BOOK_BY_ID", searchId);
                    findRequest.setClientId(clientId);
                    response = sendRequest(findRequest);
                    break;

                case "3":
                    Request getAllRequest = new Request("GET_ALL_BOOKS");
                    getAllRequest.setClientId(clientId);
                    response = sendRequest(getAllRequest);
                    break;

                case "4":
                    String removeId = readNonEmpty(scanner, "Enter Book ID: ");
                    Request removeRequest = new Request("REMOVE_BOOK", removeId);
                    removeRequest.setClientId(clientId);
                    response = sendRequest(removeRequest);
                    break;

                default:
                    System.out.println("Invalid choice");
                    continue; // skip printResponse for invalid menu input
            }

            printResponse(response);
        }

        scanner.close();
    }

    private void printResponse(Response response) {
        if (response == null) {
            System.out.println("No response received.");
            return;
        }

        System.out.println("\nSuccess: " + response.isSuccess());
        System.out.println("Message: " + response.getMessage());

        if (response.getData() != null) {
            System.out.println("Data: " + response.getData());

            if (response.getData() instanceof List<?>) {
                List<?> list = (List<?>) response.getData();
                for (Object item : list) {
                    System.out.println(item);
                }
            }
        }
    }

    public static void main(String[] args) {
        new BookClient("localhost", 6000).run();
    }
}