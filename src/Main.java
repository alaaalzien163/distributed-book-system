import models.Book;
import services.BookService;
import services.BookServiceImpl;

public class Main {
    public static void main(String[] args) {

        BookService bookService = new BookServiceImpl();

        Book book1 = new Book("B101", "Distributed Systems", "Andrew Tanenbaum", 45.5);
        Book book2 = new Book("B102", "Clean Code", "Robert C. Martin", 39.99);
        Book book3 = new Book("B103", "Head First Java", "Kathy Sierra", 29.99);

        bookService.addBook(book1);
        bookService.addBook(book2);
        bookService.addBook(book3);

        System.out.println("All Books:");
        for (Book book : bookService.getAllBooks()) {
            System.out.println(book);
        }
    }
}