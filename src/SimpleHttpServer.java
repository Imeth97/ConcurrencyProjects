import java.io.*;
import java.net.*;
import java.util.*;

public class SimpleHttpServer {

    public static void main(String[] args) throws IOException {
        int port = 8080;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Server running on http://localhost:" + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();

            new Thread(() -> {
                handleRequest(clientSocket);
            }).start();
        }
    }

    private static void handleRequest(Socket socket) {
        try {
            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();

            BufferedReader in = new BufferedReader(new InputStreamReader(input));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(output));

            String requestLine = in.readLine();

            if (requestLine == null || requestLine.isBlank()) {
                return;
            }

            System.out.println("Request: " + requestLine + " on thread: " + Thread.currentThread().getName());

            Map<String, String> headers = readHeaders(in);

            if (requestLine.startsWith("GET /events")) {
                handleSse(out);
            } else if (requestLine.startsWith("GET /ws ")) {
                WebSocketPingPongServer.handleHtmlPage(out);
            } else if (requestLine.startsWith("GET /websocket")) {
                WebSocketPingPongServer.handleWebSocket(socket, output, input, headers);
            } else {
                handleHomePage(out);
            }

        } catch (IOException e) {
            System.out.println("Client disconnected: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    private static Map<String, String> readHeaders(BufferedReader in) throws IOException {
        Map<String, String> headers = new HashMap<>();

        String line;
        while ((line = in.readLine()) != null && !line.isBlank()) {
            System.out.println("Header: " + line);

            int colonIndex = line.indexOf(":");
            if (colonIndex != -1) {
                String key = line.substring(0, colonIndex).trim().toLowerCase();
                String value = line.substring(colonIndex + 1).trim();
                headers.put(key, value);
            }
        }

        return headers;
    }

    private static void handleHomePage(BufferedWriter out) throws IOException {
        String body = """
                <!DOCTYPE html>
                <html>
                <body>
                    <h1>Simple Java HTTP Server</h1>
                    <p><a href="/ws">Open WebSocket demo</a></p>
                    <p>Try SSE with: <code>curl -N http://localhost:8080/events</code></p>
                </body>
                </html>
                """;

        out.write("HTTP/1.1 200 OK\r\n");
        out.write("Content-Type: text/html; charset=utf-8\r\n");
        out.write("Content-Length: " + body.getBytes().length + "\r\n");
        out.write("Connection: close\r\n");
        out.write("\r\n");
        out.write(body);
        out.flush();
    }

    private static void handleSse(BufferedWriter out) throws IOException, InterruptedException {
        out.write("HTTP/1.1 200 OK\r\n");
        out.write("Content-Type: text/event-stream\r\n");
        out.write("Cache-Control: no-cache\r\n");
        out.write("\r\n");
        out.flush();

        int counter = 0;

        while (true) {
            out.write("data: hello from SSE " + counter + "\n\n");
            out.flush();

            counter++;
            Thread.sleep(1000);
        }
    }
}