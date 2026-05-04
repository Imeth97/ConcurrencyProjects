import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Map;

public class WebSocketPingPongServer {

    private static final String WEBSOCKET_MAGIC_STRING =
            "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    public static void handleHtmlPage(BufferedWriter out) throws IOException {
        String body = """
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Raw Java WebSocket Demo</title>
                </head>
                <body>
                    <h1>WebSocket PING/PONG Demo</h1>

                    <input id="messageInput" placeholder="Type PING" />
                    <button onclick="sendMessage()">Send</button>

                    <h2>Messages</h2>
                    <ul id="messages"></ul>

                    <script>
                        const messages = document.getElementById("messages");
                        const input = document.getElementById("messageInput");

                        const socket = new WebSocket("ws://localhost:8080/websocket");

                        socket.onopen = () => {
                            addMessage("Connected to WebSocket server");
                        };

                        socket.onmessage = (event) => {
                            addMessage("Server: " + event.data);
                        };

                        socket.onclose = () => {
                            addMessage("WebSocket closed");
                        };

                        socket.onerror = () => {
                            addMessage("WebSocket error");
                        };

                        function sendMessage() {
                            const value = input.value;
                            socket.send(value);
                            addMessage("Client: " + value);
                            input.value = "";
                        }

                        function addMessage(text) {
                            const li = document.createElement("li");
                            li.textContent = text;
                            messages.appendChild(li);
                        }
                    </script>
                </body>
                </html>
                """;

        out.write("HTTP/1.1 200 OK\r\n");
        out.write("Content-Type: text/html; charset=utf-8\r\n");
        out.write("Content-Length: " + body.getBytes(StandardCharsets.UTF_8).length + "\r\n");
        out.write("Connection: close\r\n");
        out.write("\r\n");
        out.write(body);
        out.flush();
    }

    public static void handleWebSocket(
            Socket socket,
            OutputStream output,
            InputStream input,
            Map<String, String> headers
    ) throws IOException {
        String websocketKey = headers.get("sec-websocket-key");

        if (websocketKey == null) {
            throw new IOException("Missing Sec-WebSocket-Key header");
        }

        String acceptKey = createWebSocketAcceptKey(websocketKey);

        String response = """
                HTTP/1.1 101 Switching Protocols\r
                Upgrade: websocket\r
                Connection: Upgrade\r
                Sec-WebSocket-Accept: %s\r
                \r
                """.formatted(acceptKey);

        output.write(response.getBytes(StandardCharsets.UTF_8));
        output.flush();

        System.out.println("WebSocket handshake complete");

        while (!socket.isClosed()) {
            String message = readTextFrame(input);

            if (message == null) {
                System.out.println("WebSocket client disconnected");
                break;
            }

            System.out.println("WebSocket received: " + message);

            String reply;

            if (message.equals("PING")) {
                reply = "PONG";
            } else {
                reply = "IDK";
            }

            writeTextFrame(output, reply);
        }
    }

    private static String createWebSocketAcceptKey(String websocketKey) throws IOException {
        try {
            String combined = websocketKey + WEBSOCKET_MAGIC_STRING;

            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] hash = sha1.digest(combined.getBytes(StandardCharsets.UTF_8));

            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            throw new IOException("Failed to create WebSocket accept key", e);
        }
    }

    private static String readTextFrame(InputStream input) throws IOException {
        int firstByte = input.read();

        if (firstByte == -1) {
            return null;
        }

        int secondByte = input.read();

        if (secondByte == -1) {
            return null;
        }

        int opcode = firstByte & 0x0F;
        boolean isMasked = (secondByte & 0x80) != 0;
        long payloadLength = secondByte & 0x7F;

        // Close frame
        if (opcode == 0x8) {
            return null;
        }

        // This simple demo only handles text frames
        if (opcode != 0x1) {
            throw new IOException("Only text frames are supported");
        }

        if (payloadLength == 126) {
            payloadLength = readUnsignedShort(input);
        } else if (payloadLength == 127) {
            payloadLength = readLong(input);
        }

        byte[] maskingKey = null;

        if (isMasked) {
            maskingKey = input.readNBytes(4);
        }

        byte[] payload = input.readNBytes((int) payloadLength);

        if (isMasked) {
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) (payload[i] ^ maskingKey[i % 4]);
            }
        }

        return new String(payload, StandardCharsets.UTF_8);
    }

    private static void writeTextFrame(OutputStream output, String message) throws IOException {
        byte[] payload = message.getBytes(StandardCharsets.UTF_8);

        // First byte:
        // 10000001
        // FIN = 1, opcode = 1 text frame
        output.write(0x81);

        if (payload.length <= 125) {
            output.write(payload.length);
        } else if (payload.length <= 65535) {
            output.write(126);
            output.write((payload.length >> 8) & 0xFF);
            output.write(payload.length & 0xFF);
        } else {
            throw new IOException("Payload too large for this demo");
        }

        output.write(payload);
        output.flush();
    }

    private static int readUnsignedShort(InputStream input) throws IOException {
        int first = input.read();
        int second = input.read();

        if (first == -1 || second == -1) {
            throw new EOFException("Unexpected end of stream");
        }

        return (first << 8) | second;
    }

    private static long readLong(InputStream input) throws IOException {
        long value = 0;

        for (int i = 0; i < 8; i++) {
            int next = input.read();

            if (next == -1) {
                throw new EOFException("Unexpected end of stream");
            }

            value = (value << 8) | next;
        }

        return value;
    }
}