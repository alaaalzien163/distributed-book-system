package shared;

import models.Book;

import java.io.Serializable;

public class Request implements Serializable {
    private static final long serialVersionUID = 1L;

    private String command;
    private String bookId;
    private Book book;
    private String clientId;

    private boolean replicated;
    private String sourceServer;

    public Request() {
    }

    public Request(String command) {
        this.command = command;
    }

    public Request(String command, String bookId) {
        this.command = command;
        this.bookId = bookId;
    }

    public Request(String command, Book book) {
        this.command = command;
        this.book = book;
    }

    public Request(String command, String bookId, Book book) {
        this.command = command;
        this.bookId = bookId;
        this.book = book;
    }

    public String getCommand() {
        return command;
    }

    public String getBookId() {
        return bookId;
    }

    public Book getBook() {
        return book;
    }

    public String getClientId() {
        return clientId;
    }

    public boolean isReplicated() {
        return replicated;
    }

    public String getSourceServer() {
        return sourceServer;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public void setBookId(String bookId) {
        this.bookId = bookId;
    }

    public void setBook(Book book) {
        this.book = book;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setReplicated(boolean replicated) {
        this.replicated = replicated;
    }

    public void setSourceServer(String sourceServer) {
        this.sourceServer = sourceServer;
    }
}