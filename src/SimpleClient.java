import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.awt.Desktop;

public class SimpleClient {
    private static final Set<String> playedVideos = ConcurrentHashMap.newKeySet();
    private static final String HOST = "localhost";
    private static final int PORT = 6000;
    private static int myP2PPort = 0; //
    private static final Map<String, Long> lastSeqByRoom = new ConcurrentHashMap<>();

    // file types we can open automatically
    private static final Set<String> IMAGE_EXTS = Set.of("png", "jpg", "jpeg", "gif", "webp", "bmp");
    private static final Set<String> VIDEO_EXTS = Set.of("mp4", "webm", "mov", "avi", "mkv");

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.println("Looking for files in: " + System.getProperty("user.dir"));
        String clientId = "client-" + UUID.randomUUID().toString().substring(0, 8);
        String room = "general";

        try {
            Socket socket = new Socket(HOST, PORT);
            System.out.println("Connected to server " + HOST + ":" + PORT);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8)
            );
            PrintWriter out = new PrintWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8),
                    true
            );

            startP2PListener();    // Opens Port 7000 to listen for direct Peer files

            while (myP2PPort == 0) {
                try { Thread.sleep(10); } catch (InterruptedException e) { }
            }
            startListening(in);
            // Inside main, find the hello line and change it to:
            out.println(json(Map.of(
                    "type", "hello",
                    "clientId", clientId,
                    "p2pPort", String.valueOf(myP2PPort) // Send the port
            )));
            joinRoom(out, room);

            System.out.println("Commands: /name <id>, /room <room>, /status <message>, /mail <user> <message>, /whoami, /sendfile <path>");
            System.out.println("Type your messages:");

            while (true) {
                String line = sc.nextLine();
                if (line == null) break;

                line = line.trim();
                if (line.isEmpty()) continue;

                // P2P SEND COMMAND
                if (line.startsWith("/p2psend ")) {
                    String[] parts = line.split(" ", 3);
                    if (parts.length < 3) {
                        System.out.println("Usage: /p2psend <targetId> <filePath>");
                        continue;
                    }
                    String targetId = parts[1];
                    pendingFilePath = parts[2]; // Save the path here

                    // Signaling: Ask the server where 'targetId' is located
                    out.println(json(Map.of("type", "getPeerAddress", "targetId", targetId)));
                    System.out.println("Requesting P2P address for " + targetId + "...");
                    continue;
                }
                if (line.startsWith("/stream ")) {
                    String[] parts = line.split(" ", 3);

                    if (parts.length < 3) {
                        System.out.println("Usage: /stream <target> <filePath>");
                    } else {
                        String target = parts[1];
                        String filePath = parts[2];

                        streamVideoFile(filePath, out, clientId, room, target);
                    }
                    continue;
                }

                // MAIL COMMAND =====
                if (line.startsWith("/mail ")) {
                    // Split input into 3 parts: command, user, message
                    String[] parts = line.split(" ", 3);
                    // Check if user entered correct format
                    if (parts.length < 3) {
                        System.out.println("Usage: /mail <user> <message>");
                        continue;
                    }
                    // Get the receiver username
                    String to = parts[1];
                    // Get the message text
                    String text = parts[2];
                    // Send mail data to server as JSON
                    out.println(json(Map.of(
                            "type", "mail", // tells server this is a mail message
                            "to", to, // who should receive it
                            "clientId", clientId, // who sent it
                            "text", text // actual message
                    )));
                    // Confirm message sent
                    System.out.println("Mail sent to " + to);
                    continue;
                }

                if (line.startsWith("/sendfile ")) {
                    String filePath = line.substring(10);
                    try {
                        Path path = Path.of(filePath);
                        String name = path.getFileName().toString();
                        byte[] bytes = Files.readAllBytes(path);
                        String encoded = Base64.getEncoder().encodeToString(bytes);

                        out.println(json(Map.of(
                                "type", "file",
                                "room", room,
                                "clientId", clientId,
                                "fileName", name,
                                "data", encoded
                        )));
                        System.out.println("File sent: " + name);
                    } catch (Exception e) {
                        System.out.println("Error reading file: " + e.getMessage());
                    }
                    continue;
                }

                if (line.startsWith("/name ")) {
                    clientId = line.substring(6).trim();
                    if (clientId.isEmpty()) clientId = "client-" + UUID.randomUUID().toString().substring(0, 8);
                    out.println(json(Map.of(
                            "type", "hello",
                            "clientId", clientId,
                            "p2pPort", String.valueOf(myP2PPort)
                    )));
                    System.out.println("Name set to: " + clientId);
                    continue;
                }

                if (line.startsWith("/room ")) {
                    room = line.substring(6).trim();
                    if (room.isEmpty()) room = "general";
                    joinRoom(out, room);
                    continue;
                }

                if (line.startsWith("/status ")) {
                    String status = line.substring(8).trim();
                    out.println(json(Map.of(
                            "type", "setStatus",
                            "clientId", clientId,
                            "status", status
                    )));
                    System.out.println("Status set: " + status);
                    continue;
                }

                if (line.equals("/whoami")) {
                    System.out.println("clientId=" + clientId + " room=" + room);
                    continue;
                }

                out.println(json(Map.of(
                        "type", "send",
                        "room", room,
                        "clientId", clientId,
                        "text", line
                )));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void joinRoom(PrintWriter out, String room) {
        long sinceSeq = lastSeqByRoom.getOrDefault(room, 0L);
        out.println(json(Map.of(
                "type", "join",
                "room", room,
                "sinceSeq", sinceSeq
        )));
        System.out.println("Joined room: " + room);
    }

    private static void startListening(BufferedReader in) {
        Thread listener = new Thread(() -> {
            try {
                String raw;
                while ((raw = in.readLine()) != null) {
                    raw = raw.trim();
                    if (raw.isEmpty()) continue;

                    if (!raw.startsWith("{")) {
                        System.out.println(raw);
                        continue;
                    }

                    Map<String, String> msg = parseFlatJson(raw);
                    String type = msg.get("type");

                    if ("event".equals(type)) {
                        String room = msg.getOrDefault("room", "general");
                        long seq = parseLong(msg.get("seq"), -1);
                        String from = msg.getOrDefault("from", "?");
                        String text = msg.getOrDefault("text", "");

                        if (seq > 0) {
                            long last = lastSeqByRoom.getOrDefault(room, 0L);
                            if (seq > last) lastSeqByRoom.put(room, seq);
                        }

                        if (text != null && text.startsWith("FILE:")) {
                            String[] parts = text.split(":", 3);
                            if (parts.length == 3) {
                                handleReceivedFile(from, parts[1], parts[2]);
                            }
                        } else {
                            if (text != null && text.startsWith("VIDSTREAM:")) {
                                handleVideoStream(text);
                                continue;
                            }
                            System.out.println("[" + room + " #" + seq + "] " + from + ": " + text);
                        }
                        continue;
                    }

                    if ("statusUpdate".equals(type)) {
                        String who = msg.getOrDefault("clientId", "?");
                        String status = msg.getOrDefault("status", "");
                        System.out.println("[status] " + who + " -> " + status);
                        continue;
                    }

                    // Recieve Mail
                    if ("mail".equals(type)) {
                        // Get message text from server
                        String text = msg.getOrDefault("text", "");
                        // Display mail in console
                        System.out.println("[MAIL] " + text);
                        continue;
                    }

                    if ("peerAddress".equals(type)) {
                        String targetIp = msg.get("ip");
                        // 1. Get the port from the server message
                        int targetPort = Integer.parseInt(msg.getOrDefault("port", "7000"));
                        String targetId = msg.get("targetId");

                        if (pendingFilePath != null) {
                            try {
                                Path path = Path.of(pendingFilePath);
                                String name = path.getFileName().toString();
                                byte[] bytes = Files.readAllBytes(path);
                                String encoded = Base64.getEncoder().encodeToString(bytes);

                                // 2. Pass the targetPort as the second argument
                                System.out.println("[P2P] Connecting to " + targetIp + ":" + targetPort);
                                sendFileP2P(targetIp, targetPort, name, encoded);
                                startCameraStream(targetIp, targetPort);
                                pendingFilePath = null;
                            } catch (Exception e) {
                                System.out.println("[P2P] Error: " + e.getMessage());
                            }
                        }
                    }

                    System.out.println("Server: " + raw);
                }
            } catch (IOException e) {
                System.out.println("Connection closed.");
            }
        });
        listener.setDaemon(true);
        listener.start();
    }

    private static void handleReceivedFile(String from, String fileName, String base64Data) {
        try {
            byte[] decoded = Base64.getDecoder().decode(base64Data);
            Path savePath = Path.of("received_" + fileName);
            Files.write(savePath, decoded);

            System.out.println("\n[FILE] Received from " + from + ": " + fileName + " saved to " + savePath.toAbsolutePath());

            String ext = fileName.contains(".") ? fileName.substring(fileName.lastIndexOf('.') + 1).toLowerCase() : "";

            if (IMAGE_EXTS.contains(ext) || VIDEO_EXTS.contains(ext)) {
                new Thread(() -> {
                    try {
                        Desktop.getDesktop().open(savePath.toFile());
                    } catch (IOException e) {
                        System.out.println("[FILE] Couldn't open file: " + e.getMessage());
                    }
                }).start();
            }

        } catch (Exception e) {
            System.out.println("[FILE] Failed to save file: " + e.getMessage());
        }
    }

    // --- JSON helpers ---
    private static String json(Map<String, Object> fields) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (var e : fields.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("\"").append(escape(e.getKey())).append("\":");
            Object v = e.getValue();
            if (v == null) {
                sb.append("null");
            } else if (v instanceof Number || v instanceof Boolean) {
                sb.append(v);
            } else {
                sb.append("\"").append(escape(String.valueOf(v))).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static Map<String, String> parseFlatJson(String s) {
        Map<String, String> map = new HashMap<>();
        if (!s.startsWith("{") || !s.endsWith("}")) return map;
        s = s.substring(1, s.length() - 1).trim();
        if (s.isEmpty()) return map;

        List<String> parts = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' && (i == 0 || s.charAt(i - 1) != '\\')) inQuotes = !inQuotes;
            if (c == ',' && !inQuotes) {
                parts.add(cur.toString());
                cur.setLength(0);
            } else cur.append(c);
        }
        parts.add(cur.toString());

        for (String part : parts) {
            String[] kv = part.split(":", 2);
            if (kv.length != 2) continue;
            String key = unquote(kv[0].trim());
            String val = unquote(kv[1].trim());
            map.put(key, val);
        }
        return map;
    }

    // Inside SimpleClient.java
    private static void startP2PListener() {
        new Thread(() -> {
            try (ServerSocket p2pServer = new ServerSocket(0)) {
                myP2PPort = p2pServer.getLocalPort(); // This gets the random port
                System.out.println("P2P Listener active on port: " + myP2PPort);

                while (true) {
                    try (Socket peer = p2pServer.accept();
                         BufferedReader in = new BufferedReader(new InputStreamReader(peer.getInputStream()))) {
                        String fileData = in.readLine();
                        if (fileData != null && fileData.startsWith("P2P_FILE|")) {
                            String[] parts = fileData.split("\\|", 3);
                            handleReceivedFile("Peer", parts[1], parts[2]);
                        }
                        if (fileData != null && fileData.startsWith("CAMERA|")) {
                            String frame = fileData.split("\\|", 2)[1];
                            System.out.println("[CAMERA FRAME] " + frame.substring(0, Math.min(30, frame.length())));
                        }

                    }
                }
            } catch (IOException e) {
                System.out.println("P2P Listener Error: " + e.getMessage());
            }
        }).start();
    }

    // When you want to send a file P2P
    public static void sendFileP2P(String targetIP, int port, String fileName, String base64) {
        try (Socket directSocket = new Socket(targetIP, port);
             PrintWriter peerOut = new PrintWriter(directSocket.getOutputStream(), true)) {

            peerOut.println("P2P_FILE|" + fileName + "|" + base64);
            System.out.println("P2P Transfer Complete to port " + port + "!");
        } catch (IOException e) {
            System.out.println("P2P Connection Failed: " + e.getMessage());
        }
    }

    private static String unquote(String s) {
        s = s.trim();
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) {
            s = s.substring(1, s.length() - 1);
        }
        return s.replace("\\\"", "\"").replace("\\\\", "\\");
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static long parseLong(String s, long def) {
        try { return Long.parseLong(s); } catch (Exception e) { return def; }
    }

    private static String pendingFilePath = null; // Stores the path for P2P transfer

    private static final Map<String, List<byte[]>> videoChunks = new ConcurrentHashMap<>();
    // Store video chunks temporarily while receiving
    public static void streamVideoFile(String filePath, PrintWriter out, String clientId, String room, String target){
        // Run streaming in a separate thread
        new Thread(() -> {
            try {
                // Read full video file
                Path path = Path.of(filePath);
                byte[] data = Files.readAllBytes(path);
                // Define chunk size (small pieces of video)
                int chunkSize = 40000;
                // Calculate total number of chunks
                int total = (int) Math.ceil((double) data.length / chunkSize);
                // Loop through file and send chunk by chunk
                for (int i = 0; i < total; i++) {
                    int start = i * chunkSize;
                    int end = Math.min(start + chunkSize, data.length);
                    // Extract small part of video
                    byte[] chunk = Arrays.copyOfRange(data, start, end);
                    // Convert binary data to Base64 string
                    String encoded = Base64.getEncoder().encodeToString(chunk);
                    // Create streaming message
                    String msg = "VIDSTREAM:" + path.getFileName() + ":" + i + ":" + total + ":" + encoded;
                    // Send chunk to server
                    out.println(json(Map.of(
                            "type", "send",
                            "room", room,
                            "clientId", clientId,
                            "to", target,
                            "text", msg

                    )));
                    // Small delay to simulate real-time streaming
                    Thread.sleep(40); // simulate streaming
                }

                System.out.println("[STREAM] Video streaming finished");

            } catch (Exception e) {
                System.out.println("[STREAM ERROR] " + e.getMessage());
            }
        }).start();
    }

    private static void handleVideoStream(String text) {
        try {
            // Split incoming message into parts
            String[] parts = text.split(":", 5);

            String fileName = parts[1];
            int index = Integer.parseInt(parts[2]); // chunk number
            int total = Integer.parseInt(parts[3]); // total chunks
            // Decode Base64 back to bytes
            byte[] data = Base64.getDecoder().decode(parts[4]);
            // Create list if not already created
            videoChunks.putIfAbsent(fileName, new ArrayList<>());
            List<byte[]> list = videoChunks.get(fileName);
            // Ensure list size matches index
            while (list.size() <= index) list.add(null);
            // Store chunk in correct position
            list.set(index, data);
            // If all chunks received → rebuild file
            if (list.size() == total && !list.contains(null)) {

                if (playedVideos.contains(fileName)) return;
                playedVideos.add(fileName);

                ByteArrayOutputStream output = new ByteArrayOutputStream();

                for (int i = 0; i < total; i++) {
                    byte[] c = list.get(i);
                    if (c == null) return;
                    output.write(c);
                }

                Path file = Path.of("streamed_" + fileName);
                Files.write(file, output.toByteArray());
                System.out.println("[STREAM RECEIVED] " + fileName);

                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {}

                Desktop.getDesktop().open(file.toFile());

                videoChunks.remove(fileName); // cleanup
            }

        } catch (Exception e) {
            System.out.println("[STREAM RECEIVE ERROR] " + e.getMessage());
        }
    }
    // Camera Stream

    public static void startCameraStream(String targetIp, int port) {
        // Run camera streaming in a separate thread
        new Thread(() -> {
            try ( // Connect directly to other client
                  Socket socket = new Socket(targetIp, port);
                  // Output stream to send data
                  PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                System.out.println("[CAMERA] Streaming started...");
                // Keep sending frames continuously
                while (true) {
                    // Simulate a camera frame using timestamp
                    String fakeFrame = Base64.getEncoder().encodeToString(
                            ("frame_" + System.currentTimeMillis()).getBytes()
                    );
                    // Send frame to other client
                    out.println("CAMERA|" + fakeFrame);
                    // Delay between frames
                    Thread.sleep(100); // simulate FPS
                }

            } catch (Exception e) {
                System.out.println("[CAMERA ERROR] " + e.getMessage());
            }
        }).start();
    }
}