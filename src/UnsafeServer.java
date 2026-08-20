import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UnsafeServer {
    private static final int PORT = 6000;

    // Room map itself is still concurrent (so the server doesn't crash on startup),
    // but the CONTENTS of each RoomState are unsafe.
    private static final Map<String, RoomState> rooms = new ConcurrentHashMap<>();

    private static final Map<String, String> statusByClientId = new ConcurrentHashMap<>();

    private static final Map<String, List<String>> mailboxes = new ConcurrentHashMap<>();

    // ==================== CLIENT CONNECTION ====================
    static class ClientConn {
        final Socket socket;
        final PrintWriter out;
        String clientId;
        String room = "general";

        ClientConn(Socket socket, PrintWriter out) {
            this.socket = socket;
            this.out = out;
            this.clientId = "client-" + socket.getPort();
        }
    }

    // ==================== EVENT ====================
    static class Event {
        final long seq;
        final String room;
        final String from;
        final String text;
        final long serverTimeMs;

        Event(long seq, String room, String from, String text, long serverTimeMs) {
            this.seq = seq;
            this.room = room;
            this.from = from;
            this.text = text;
            this.serverTimeMs = serverTimeMs;
        }
    }

    // ==================== ROOM STATE (ALL UNSAFE) ====================
    static class RoomState {
        final String name;

        // UNSAFE: plain long instead of AtomicLong
        long nextSeq = 0;

        // UNSAFE: plain HashSet instead of ConcurrentHashMap.newKeySet()
        final Set<ClientConn> members = new HashSet<>();

        // UNSAFE: plain ArrayList instead of CopyOnWriteArrayList
        final List<Event> history = new ArrayList<>();

        RoomState(String name) { this.name = name; }
    }

    // ==================== BANNED WORDS (UNSAFE) ====================
    // UNSAFE: plain HashSet instead of ConcurrentHashMap-backed set
    static final Set<String> bannedWords = new HashSet<>();

    static {
        bannedWords.add("spam");
        bannedWords.add("curse");
    }

    // ==================== MAIN ====================
    public static void main(String[] args) {
        System.out.println("[UNSAFE] Server starting on port " + PORT + "...");

        // Background thread modifies bannedWords while clients use it
        new Thread(() -> {
            try {
                Thread.sleep(15000);
                bannedWords.add("crash" + System.currentTimeMillis());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // ==================== CLIENT HANDLER ====================
    private static void handleClient(Socket socket) {
        ClientConn conn = null;

        try (
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)
        ) {
            conn = new ClientConn(socket, out);
            joinRoom(conn, "general");
            out.println("Welcome! Moderation is ACTIVE. [UNSAFE MODE]");

            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                if (!line.startsWith("{")) {
                    String filtered = filterMessage(line);
                    Event ev = appendEvent(conn.room, conn.clientId, filtered);
                    broadcast(ev);
                    continue;
                }

                Map<String, String> msg = parseFlatJson(line);
                String type = msg.get("type");

                if (type == null) {
                    out.println(errorJson("Missing 'type'"));
                    continue;
                }

                switch (type) {
                    case "hello" -> {
                        conn.clientId = msg.getOrDefault("clientId", conn.clientId);
                        String p2pPort = msg.getOrDefault("p2pPort", "7000");
                        statusByClientId.put(conn.clientId, p2pPort);
                        out.println(json(Map.of("type", "welcome", "clientId", conn.clientId)));
                        deliverMailbox(conn);
                    }

                    case "join" -> {
                        String room = msg.getOrDefault("room", "general");
                        long sinceSeq = parseLong(msg.get("sinceSeq"), 0);
                        joinRoom(conn, room);
                        sendHistory(conn, room, sinceSeq);
                        out.println(json(Map.of("type", "joined", "room", room)));
                    }

                    case "send" -> {
                        String room = msg.getOrDefault("room", conn.room);
                        String from = msg.getOrDefault("clientId", conn.clientId);
                        String text = msg.getOrDefault("text", "");

                        joinRoom(conn, room);

                        String filtered;
                        if (text != null && text.startsWith("VIDSTREAM:")) {
                            filtered = text;
                        } else {
                            filtered = filterMessage(text);
                        }

                        Event ev = appendEvent(room, from, filtered);
                        broadcast(ev);
                    }

                    case "mail" -> {
                        String to = msg.get("to");
                        String from = msg.getOrDefault("clientId", conn.clientId);
                        String text = msg.getOrDefault("text", "");

                        // UNSAFE: plain ArrayList, no synchronization
                        mailboxes.putIfAbsent(to, new ArrayList<>());
                        mailboxes.get(to).add(from + ": " + text);

                        out.println(json(Map.of("type", "mailStored", "to", to)));
                    }

                    case "setStatus" -> {
                        String clientId = msg.getOrDefault("clientId", conn.clientId);
                        String status = msg.getOrDefault("status", "");
                        conn.clientId = clientId;
                        statusByClientId.put(clientId, status);
                        broadcastStatus(conn.room, clientId, status);
                    }

                    case "file" -> {
                        String room = msg.getOrDefault("room", conn.room);
                        String from = msg.getOrDefault("clientId", conn.clientId);
                        String fileName = msg.getOrDefault("fileName", "file");
                        String base64Data = msg.getOrDefault("data", "");
                        Event ev = appendEvent(room, from, "FILE:" + fileName + ":" + base64Data);
                        broadcast(ev);
                    }

                    case "getPeerAddress" -> {
                        String targetId = msg.get("targetId");
                        String port = statusByClientId.getOrDefault(targetId, "0");
                        String targetIp = null;
                        for (RoomState r : rooms.values()) {
                            for (ClientConn c : r.members) {
                                if (c.clientId.equals(targetId)) {
                                    targetIp = c.socket.getInetAddress().getHostAddress();
                                    break;
                                }
                            }
                        }
                        if (targetIp != null && !port.equals("0")) {
                            out.println(json(Map.of(
                                    "type", "peerAddress", "targetId", targetId,
                                    "ip", targetIp, "port", port)));
                        } else {
                            out.println(errorJson("Target user not found or port not registered"));
                        }
                    }

                    default -> out.println(errorJson("Unknown type: " + type));
                }
            }

        } catch (IOException e) {
            System.out.println("Disconnected: " + socket.getRemoteSocketAddress());
        } finally {
            if (conn != null) leaveRoom(conn);
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    // ==================== MAILBOX DELIVERY ====================
    private static void deliverMailbox(ClientConn conn) {
        List<String> inbox = mailboxes.get(conn.clientId);
        if (inbox == null) return;
        // UNSAFE: no synchronization on the list
        for (String msg : inbox) {
            conn.out.println(json(Map.of("type", "mail", "text", msg)));
        }
        inbox.clear();
    }

    // ==================== ROOM MANAGEMENT ====================
    static void joinRoom(ClientConn conn, String roomName) {
        if (conn.room != null && !conn.room.equals(roomName)) {
            RoomState old = rooms.get(conn.room);
            if (old != null) old.members.remove(conn);
        }
        conn.room = roomName;
        RoomState room = rooms.computeIfAbsent(roomName, RoomState::new);
        room.members.add(conn);
    }

    private static void leaveRoom(ClientConn conn) {
        RoomState room = rooms.get(conn.room);
        if (room != null) room.members.remove(conn);
    }

    // ==================== EVENT APPEND (UNSAFE) ====================
    static Event appendEvent(String roomName, String from, String text) {
        RoomState room = rooms.computeIfAbsent(roomName, RoomState::new);

        // UNSAFE: read-then-write on a plain long — classic race condition
        long seq = room.nextSeq + 1;
        room.nextSeq = seq;

        Event ev = new Event(seq, roomName, from, text, System.currentTimeMillis());
        room.history.add(ev);
        return ev;
    }

    private static void sendHistory(ClientConn conn, String roomName, long sinceSeq) {
        RoomState room = rooms.computeIfAbsent(roomName, RoomState::new);
        for (Event ev : room.history) {
            if (ev.seq > sinceSeq) conn.out.println(eventJson(ev));
        }
    }

    // ==================== BROADCAST (UNSAFE) ====================
    static void broadcast(Event ev) {
        RoomState room = rooms.get(ev.room);
        if (room == null) return;
        String payload = eventJson(ev);
        // UNSAFE: iterating a plain HashSet while other threads modify it
        for (ClientConn member : room.members) {
            member.out.println(payload);
        }
    }

    private static void broadcastStatus(String roomName, String clientId, String status) {
        RoomState room = rooms.get(roomName);
        if (room == null) return;
        String payload = json(Map.of(
                "type", "statusUpdate", "room", roomName,
                "clientId", clientId, "status", status));
        for (ClientConn member : room.members) {
            member.out.println(payload);
        }
    }

    // ==================== FILTER (UNSAFE) ====================
    static String filterMessage(String message) {
        String result = message;
        // UNSAFE: iterating a plain HashSet while another thread adds to it
        for (String word : bannedWords) {
            if (result.toLowerCase().contains(word.toLowerCase())) {
                result = result.replaceAll(
                        "(?i)" + java.util.regex.Pattern.quote(word), "***");
            }
        }
        return result;
    }

    // ==================== JSON HELPERS ====================
    private static String eventJson(Event ev) {
        return json(Map.of(
                "type", "event", "room", ev.room,
                "seq", ev.seq, "from", ev.from,
                "text", ev.text, "serverTimeMs", ev.serverTimeMs));
    }

    private static String errorJson(String message) {
        return json(Map.of("type", "error", "message", message));
    }

    private static String json(Map<String, Object> fields) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (var e : fields.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("\"").append(e.getKey()).append("\":");
            Object v = e.getValue();
            if (v instanceof Number) sb.append(v);
            else sb.append("\"").append(v).append("\"");
        }
        sb.append("}");
        return sb.toString();
    }

    private static Map<String, String> parseFlatJson(String s) {
        Map<String, String> map = new HashMap<>();
        s = s.substring(1, s.length() - 1);
        String[] parts = s.split(",");
        for (String p : parts) {
            String[] kv = p.split(":", 2);
            if (kv.length == 2) {
                String key = kv[0].replace("\"", "");
                String val = kv[1].replace("\"", "");
                map.put(key.trim(), val.trim());
            }
        }
        return map;
    }

    private static long parseLong(String s, long def) {
        try { return Long.parseLong(s); } catch (Exception e) { return def; }
    }
}