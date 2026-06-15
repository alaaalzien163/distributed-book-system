package server;

import models.Book;
import services.BookService;
import services.BookServiceImpl;
import shared.Request;
import shared.Response;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BookServer {

    private enum Role {
        LEADER,
        FOLLOWER
    }

    private static class PeerServer {
        private final String name;
        private final String host;
        private final int port;
        private final int priority;

        public PeerServer(String name, String host, int port, int priority) {
            this.name = name;
            this.host = host;
            this.port = port;
            this.priority = priority;
        }
    }

    private final int port;
    private final String serverName;
    private final int priority;
    private final BookService bookService;
    private final ExecutorService clientPool;
    private final ScheduledExecutorService clusterScheduler;
    private final List<PeerServer> peers = new ArrayList<>();
    private volatile int currentLeaderPriority = -1;

    private volatile Role role = Role.FOLLOWER;
    private volatile String currentLeaderName = null;
    private volatile boolean electionInProgress = false;
    private volatile long lastLeaderHeartbeat = System.currentTimeMillis();

    private static final long HEARTBEAT_INTERVAL_MS = 2000;
    private static final long HEARTBEAT_TIMEOUT_MS = 6000;

    public BookServer(int port, String serverName) {
        this.port = port;
        this.serverName = serverName;
        this.priority = resolvePriority(serverName);
        this.bookService = new BookServiceImpl();
        this.clientPool = Executors.newCachedThreadPool();
        this.clusterScheduler = Executors.newScheduledThreadPool(2);

        peers.add(new PeerServer("Server-1", "localhost", 5001, 1));
        peers.add(new PeerServer("Server-2", "localhost", 5002, 2));
        peers.add(new PeerServer("Server-3", "localhost", 5003, 3));

        peers.removeIf(p -> p.port == this.port);
    }

    private int resolvePriority(String serverName) {
        return switch (serverName) {
            case "Server-1" -> 1;
            case "Server-2" -> 2;
            case "Server-3" -> 3;
            default -> 0;
        };
    }

    public void start() {
        System.out.println("[" + serverName + "] Book Server started on port " + port + " | priority=" + priority);

        startClusterTasks();
        //clusterScheduler.schedule(this::startElection, 3, TimeUnit.SECONDS);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientPool.submit(() -> handleClient(clientSocket));
            }
        } catch (Exception e) {
            System.out.println("[" + serverName + "] Server error: " + e.getMessage());
        }
    }

    private void startClusterTasks() {
        clusterScheduler.scheduleAtFixedRate(() -> {
            try {
                if (role == Role.LEADER) {
                    sendHeartbeats();
                } else {
                    monitorLeader();
                }
            } catch (Exception ignored) {
            }
        }, 2, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void monitorLeader() {
        if (role == Role.LEADER) {
            return;
        }

        if (currentLeaderName == null) {
            return;
        }

        long elapsed = System.currentTimeMillis() - lastLeaderHeartbeat;

        if (elapsed > HEARTBEAT_TIMEOUT_MS) {
            System.out.println("[" + serverName + "] Leader timeout detected. Starting election...");
            startElection();
        }
    }

    private synchronized void startElection() {
        if (electionInProgress) {
            return;
        }

        electionInProgress = true;

        try {
            System.out.println("[" + serverName + "] Starting election...");

            boolean higherPriorityAlive = false;

            for (PeerServer peer : peers) {
                if (peer.priority > this.priority) {
                    try {
                        Request election = new Request("ELECTION");
                        election.setSourceServer(serverName);

                        Response response = sendControlMessage(peer, election);

                        if (response != null && response.isSuccess()) {
                            higherPriorityAlive = true;
                            System.out.println("[" + serverName + "] Higher priority server " + peer.name + " is alive.");
                            break;
                        }
                    } catch (Exception ignored) {
                    }
                }
            }

            if (!higherPriorityAlive) {
                becomeLeader();
            } else {
                System.out.println("[" + serverName + "] Waiting for coordinator announcement...");
            }

        } finally {
            electionInProgress = false;
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        currentLeaderName = serverName;
        currentLeaderPriority = priority;
        lastLeaderHeartbeat = System.currentTimeMillis();

        System.out.println("[" + serverName + "] I am the NEW LEADER.");

        announceCoordinator();
    }

    private void announceCoordinator() {
        for (PeerServer peer : peers) {
            try {
                Request coordinator = new Request("COORDINATOR");
                coordinator.setSourceServer(serverName);

                sendControlMessage(peer, coordinator);

                System.out.println("[" + serverName + "] Sent COORDINATOR to " + peer.name);
            } catch (Exception e) {
                System.out.println("[" + serverName + "] Failed to notify " + peer.name + ": " + e.getMessage());
            }
        }
    }

    private void sendHeartbeats() {
        for (PeerServer peer : peers) {
            try {
                Request heartbeat = new Request("HEARTBEAT");
                heartbeat.setSourceServer(serverName);

                sendControlMessage(peer, heartbeat);
            } catch (Exception ignored) {
            }
        }
    }

    private Response sendControlMessage(PeerServer peer, Request request) throws Exception {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(peer.host, peer.port), 1500);

            try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                out.flush();
                out.writeObject(request);
                out.flush();

                return (Response) in.readObject();
            }
        }
    }

    private void handleClient(Socket socket) {
        try (socket;
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.flush();

            Request request = (Request) in.readObject();
            System.out.println("[" + serverName + "] Received request: " + request.getCommand());

            Response response = processRequest(request);

            out.writeObject(response);
            out.flush();

        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg != null && (msg.contains("aborted") || msg.contains("reset"))) {
                return;
            }

            System.out.println("[" + serverName + "] Client handling error: "
                    + e.getClass().getSimpleName() + " - " + msg);
        }
    }

    private Response processRequest(Request request) {
        if (request == null || request.getCommand() == null) {
            return new Response(false, "Invalid request", null);
        }

        try {
            switch (request.getCommand()) {

                case "ELECTION":
                    return handleElectionMessage(request);

                case "COORDINATOR":
                    int incomingPriority = resolvePriority(request.getSourceServer());

                    if (incomingPriority >= currentLeaderPriority) {
                        currentLeaderName = request.getSourceServer();
                        currentLeaderPriority = incomingPriority;
                        role = Role.FOLLOWER;
                        electionInProgress = false;
                        lastLeaderHeartbeat = System.currentTimeMillis();

                        System.out.println("[" + serverName + "] New leader is " + currentLeaderName);
                    } else {
                        System.out.println("[" + serverName + "] Ignored stale COORDINATOR from " + request.getSourceServer());
                    }

                    return new Response(true, "ACK", null);

                case "HEARTBEAT":
                    int hbPriority = resolvePriority(request.getSourceServer());

                    if (request.getSourceServer() != null &&
                            (currentLeaderName == null || hbPriority >= currentLeaderPriority)) {
                        currentLeaderName = request.getSourceServer();
                        currentLeaderPriority = hbPriority;
                        lastLeaderHeartbeat = System.currentTimeMillis();
                    }

                    return new Response(true, "ALIVE", null);
                case "ADD_BOOK":
                    if (role == Role.LEADER) {
                        bookService.addBook(request.getBook());

                        if (!request.isReplicated()) {
                            replicateAddToFollowers(request);
                        }

                        return new Response(true, "[" + serverName + "] Book added successfully", request.getBook());
                    } else {
                        return forwardToLeader(request);
                    }

                case "REPLICA_ADD_BOOK":
                    bookService.addBook(request.getBook());
                    return new Response(true, "[" + serverName + "] Replica applied", request.getBook());

                case "FIND_BOOK_BY_ID":
                    Book foundBook = bookService.findBookById(request.getBookId());
                    if (foundBook != null) {
                        return new Response(true, "[" + serverName + "] Book found", foundBook);
                    }
                    return new Response(false, "[" + serverName + "] Book not found", null);

                case "GET_ALL_BOOKS":
                    return new Response(true, "[" + serverName + "] All books retrieved", bookService.getAllBooks());

                case "REMOVE_BOOK":
                    if (role == Role.LEADER) {
                        boolean removed = bookService.removeBook(request.getBookId());

                        if (removed && !request.isReplicated()) {
                            replicateRemoveToFollowers(request);
                        }

                        return new Response(removed,
                                "[" + serverName + "] " + (removed ? "Book removed successfully" : "Book not found"),
                                removed);
                    } else {
                        return forwardToLeader(request);
                    }

                case "REPLICA_REMOVE_BOOK":
                    boolean removedReplica = bookService.removeBook(request.getBookId());
                    return new Response(removedReplica, "[" + serverName + "] Replica remove applied", removedReplica);

                default:
                    return new Response(false, "[" + serverName + "] Unknown command: " + request.getCommand(), null);
            }
        } catch (Exception e) {
            return new Response(false, "[" + serverName + "] Error: " + e.getMessage(), null);
        }
    }

    private Response handleElectionMessage(Request request) {
        int requesterPriority = resolvePriority(request.getSourceServer());

        if (this.priority > requesterPriority) {
            System.out.println("[" + serverName + "] Received ELECTION from lower server. Replying OK and starting own election.");
            clusterScheduler.execute(this::startElection);
            return new Response(true, "OK", null);
        }

        return new Response(false, "LOWER_PRIORITY", null);
    }

    private void replicateAddToFollowers(Request originalRequest) {
        for (PeerServer peer : peers) {
            try {
                Request replica = new Request("REPLICA_ADD_BOOK");
                replica.setBook(originalRequest.getBook());
                replica.setReplicated(true);
                replica.setSourceServer(serverName);
                replica.setClientId(originalRequest.getClientId());

                sendControlMessage(peer, replica);

                System.out.println("[" + serverName + "] Replicated ADD_BOOK to " + peer.name);
            } catch (Exception e) {
                System.out.println("[" + serverName + "] Failed to replicate to " + peer.name + ": " + e.getMessage());
            }
        }
    }

    private void replicateRemoveToFollowers(Request originalRequest) {
        for (PeerServer peer : peers) {
            try {
                Request replica = new Request("REPLICA_REMOVE_BOOK");
                replica.setBookId(originalRequest.getBookId());
                replica.setReplicated(true);
                replica.setSourceServer(serverName);
                replica.setClientId(originalRequest.getClientId());

                sendControlMessage(peer, replica);

                System.out.println("[" + serverName + "] Replicated REMOVE_BOOK to " + peer.name);
            } catch (Exception e) {
                System.out.println("[" + serverName + "] Failed to replicate remove to " + peer.name + ": " + e.getMessage());
            }
        }
    }

    private Response forwardToLeader(Request request) {
        PeerServer leader = getKnownLeaderPeer();

        if (leader == null) {
            startElection();
            sleepQuietly(500);
            leader = getKnownLeaderPeer();
        }

        if (leader == null) {
            return new Response(false, "[" + serverName + "] Leader unavailable", null);
        }

        try {
            return sendRequestToPeer(leader, request);
        } catch (Exception e) {
            currentLeaderName = null;
            return new Response(false, "[" + serverName + "] Cannot reach leader: " + e.getMessage(), null);
        }
    }

    private Response sendRequestToPeer(PeerServer peer, Request request) throws Exception {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(peer.host, peer.port), 1500);

            try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                out.flush();
                out.writeObject(request);
                out.flush();

                return (Response) in.readObject();
            }
        }
    }

    private PeerServer getKnownLeaderPeer() {
        if (role == Role.LEADER) {
            return new PeerServer(serverName, "localhost", port, priority);
        }

        if (currentLeaderName != null) {
            if (currentLeaderName.equals(serverName)) {
                return new PeerServer(serverName, "localhost", port, priority);
            }

            for (PeerServer peer : peers) {
                if (peer.name.equals(currentLeaderName)) {
                    return peer;
                }
            }
        }

        return null;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        int port = 5001;
        String serverName = "Server-1";

        if (args.length >= 1) {
            port = Integer.parseInt(args[0]);
        }
        if (args.length >= 2) {
            serverName = args[1];
        }

        new BookServer(port, serverName).start();
    }
}