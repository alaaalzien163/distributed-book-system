package services;

import models.Book;
import java.util.List;

public interface BookService {
    void addBook(Book book);
    Book findBookById(String id);
    List<Book> getAllBooks();
    boolean removeBook(String id);
}