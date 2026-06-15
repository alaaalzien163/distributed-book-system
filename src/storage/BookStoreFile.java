package storage;

import models.Book;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

public final class BookStoreFile {

    private static final Path STORE_PATH = Paths.get("data", "books.ser");

    private BookStoreFile() {
    }

    public static void bootstrapIfEmpty() {
        synchronized (BookStoreFile.class) {
            try {
                ensureStorageReady();

                try (RandomAccessFile raf = new RandomAccessFile(STORE_PATH.toFile(), "rw");
                     FileChannel channel = raf.getChannel();
                     FileLock lock = channel.lock()) {

                    if (raf.length() > 0) {
                        return;
                    }

                    Map<String, Book> defaults = createDefaultBooks();
                    writeMapToFile(raf, defaults);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to bootstrap book store: " + e.getMessage(), e);
            }
        }
    }

    public static Map<String, Book> loadBookMap() {
        synchronized (BookStoreFile.class) {
            try {
                ensureStorageReady();

                try (RandomAccessFile raf = new RandomAccessFile(STORE_PATH.toFile(), "rw");
                     FileChannel channel = raf.getChannel();
                     FileLock lock = channel.lock()) {

                    if (raf.length() == 0) {
                        return new LinkedHashMap<>();
                    }

                    byte[] data = new byte[(int) raf.length()];
                    raf.seek(0);
                    raf.readFully(data);

                    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
                        Object obj = ois.readObject();

                        if (obj instanceof Map<?, ?> rawMap) {
                            Map<String, Book> result = new LinkedHashMap<>();
                            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                                if (entry.getKey() instanceof String && entry.getValue() instanceof Book) {
                                    result.put((String) entry.getKey(), (Book) entry.getValue());
                                }
                            }
                            return result;
                        }

                        return new LinkedHashMap<>();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to load books: " + e.getMessage(), e);
            }
        }
    }

    public static void saveBookMap(Map<String, Book> books) {
        synchronized (BookStoreFile.class) {
            try {
                ensureStorageReady();

                try (RandomAccessFile raf = new RandomAccessFile(STORE_PATH.toFile(), "rw");
                     FileChannel channel = raf.getChannel();
                     FileLock lock = channel.lock()) {

                    writeMapToFile(raf, books);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to save books: " + e.getMessage(), e);
            }
        }
    }

    private static void writeMapToFile(RandomAccessFile raf, Map<String, Book> books) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(new LinkedHashMap<>(books));
            oos.flush();
        }

        byte[] data = bos.toByteArray();
        raf.setLength(0);
        raf.seek(0);
        raf.write(data);
        raf.getFD().sync();
    }

    private static void ensureStorageReady() throws IOException {
        Path parent = STORE_PATH.getParent();
        if (parent != null && Files.notExists(parent)) {
            Files.createDirectories(parent);
        }
        if (Files.notExists(STORE_PATH)) {
            Files.createFile(STORE_PATH);
        }
    }

    private static Map<String, Book> createDefaultBooks() {
        Map<String, Book> defaults = new LinkedHashMap<>();
        defaults.put("B101", new Book("B101", "Distributed Systems", "Andrew Tanenbaum", 45.5));
        defaults.put("B102", new Book("B102", "Clean Code", "Robert C. Martin", 39.99));
        defaults.put("B103", new Book("B103", "Head First Java", "Kathy Sierra", 29.99));
        return defaults;
    }
}