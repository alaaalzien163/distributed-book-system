package loadbalancer;

import shared.Request;
import shared.Response;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;


public class LoadBalancer {

    private static class ServerNode {
        private final String name;
        private final String host;
        private final int port;
        private volatile boolean healthy = true;
        private volatile int activeConnections = 0;

        public ServerNode(String name, String host, int port) {
            this.name = name;
            this.host = host;
            this.port = port;
        }
    }

    private final int port;
    private final List<ServerNode> servers = new CopyOnWriteArrayList<>();
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    private final ConcurrentHashMap<String, ServerNode> stickySessions = new ConcurrentHashMap<>();
    private final ExecutorService clientPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService healthChecker = Executors.newSingleThreadScheduledExecutor();

    public LoadBalancer(int port) {
        this.port = port;

        // backend servers
        servers.add(new ServerNode("Server-1", "localhost", 5001));
        servers.add(new ServerNode("Server-2", "localhost", 5002));
        servers.add(new ServerNode("Server-3", "localhost", 5003));

        System.out.println("Available servers:");
        for (ServerNode server : servers) {
            System.out.println(server.name + " -> " + server.port);
        }

        startHealthChecks();
    }

    public void start() {
        System.out.println("[LoadBalancer] Started on port " + port);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientPool.submit(() -> handleClient(clientSocket));
            }
        } catch (Exception e) {
            System.out.println("[LoadBalancer] Error: " + e.getMessage());
        }
    }

    private void handleClient(Socket clientSocket) {
        try (Socket client = clientSocket;
             ObjectOutputStream out = new ObjectOutputStream(client.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(client.getInputStream())) {

            out.flush();

            Request request = (Request) in.readObject();
            System.out.println("[LoadBalancer] Received request: " + request.getCommand());

            Response response = forwardRequest(request);

            out.writeObject(response);
            out.flush();

        } catch (Exception e) {
            System.out.println("[LoadBalancer] Client handling error: " + e.getMessage());
        }
    }

    private Response forwardRequest(Request request) {

        String clientId = request.getClientId();

        ServerNode target = null;

        // Sticky Session Lookup
        if (clientId != null) {

            target = stickySessions.get(clientId);

            // إذا السيرفر القديم مات
            if (target != null && !target.healthy) {
                stickySessions.remove(clientId);
                target = null;
            }
        }

        // إذا لا يوجد Session سابق
        if (target == null) {

            target = getRoundRobinServer();
            if (target == null) {
                return new Response(false,
                        "No healthy backend servers available",
                        null);
            }

            // إنشاء Session جديدة
            if (clientId != null) {
                stickySessions.put(clientId, target);
            }
        }

        try {

            target.activeConnections++;

            System.out.println(
                    "[StickySession] Client "
                            + clientId
                            + " -> "
                            + target.name
            );

            return sendToBackend(target, request);

        } catch (Exception e) {

            target.healthy = false;

            return new Response(false,
                    "Backend server failed: " + target.name,
                    null);

        } finally {

            target.activeConnections--;
        }
    }
    private Response sendToBackend(ServerNode server, Request request) throws Exception {
        try (Socket backendSocket = new Socket()) {
            backendSocket.connect(new InetSocketAddress(server.host, server.port), 1500);

            try (ObjectOutputStream out = new ObjectOutputStream(backendSocket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(backendSocket.getInputStream())) {

                out.flush();
                out.writeObject(request);
                out.flush();

                return (Response) in.readObject();
            }
        }
    }

    private ServerNode getRoundRobinServer() {

        List<ServerNode> healthyServers = new ArrayList<>();

        for (ServerNode server : servers) {
            if (server.healthy) {
                healthyServers.add(server);
            }
        }

        if (healthyServers.isEmpty()) {
            return null;
        }

        int index =
                roundRobinIndex.getAndIncrement()
                        % healthyServers.size();

        return healthyServers.get(index);
    }

    private void startHealthChecks() {
        healthChecker.scheduleAtFixedRate(() -> {
            for (ServerNode server : servers) {
                boolean alive = isServerAlive(server.host, server.port);

                if (alive && !server.healthy) {
                    System.out.println("[HealthCheck] " + server.name + " is back online.");
                }

                if (!alive && server.healthy) {
                    System.out.println("[HealthCheck] " + server.name + " is DOWN.");
                }

                server.healthy = alive;
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private boolean isServerAlive(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static void main(String[] args) {
        int port = 6000;
        if (args.length >= 1) {
            port = Integer.parseInt(args[0]);
        }
        new LoadBalancer(port).start();
    }
}