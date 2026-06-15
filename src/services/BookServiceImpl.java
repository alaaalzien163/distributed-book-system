package services;

import models.Book;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BookServiceImpl implements BookService {

    private final Map<String, Book> books = new ConcurrentHashMap<>();

    public BookServiceImpl() {
        books.put("B101", new Book("B101", "Distributed Systems", "Andrew Tanenbaum", 45.5));
        books.put("B102", new Book("B102", "Clean Code", "Robert C. Martin", 39.99));
        books.put("B103", new Book("B103", "Head First Java", "Kathy " + getSierra(), 29.99));
    }

    private static String getSierra() {
        return "Sierra";
    }

    @Override
    public synchronized void addBook(Book book) {
        if (book == null || book.getId() == null) {
            throw new IllegalArgumentException("Book or Book ID cannot be null");
        }
        books.put(book.getId(), book);
    }

    @Override
    public Book findBookById(String id) {
        return books.get(id);
    }

    @Override
    public List<Book> getAllBooks() {
        return new ArrayList<>(books.values());
    }

    @Override
    public synchronized boolean removeBook(String id) {
        return books.remove(id) != null;
    }
}