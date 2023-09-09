import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class MainTest {

    public static void main(String[] args) {
        int port = 8080; // 指定监听的端口号

        try {
            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("服务器已启动，正在监听端口 " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("客户端已连接，连接地址: " + clientSocket.getInetAddress());

                // 创建一个线程来处理客户端连接
                ClientHandler clientHandler = new ClientHandler(clientSocket);
                clientHandler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class ClientHandler extends Thread {
        private Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        public void run() {
            try {
                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream();

                // 在这里处理客户端的请求和响应

                // 当客户端主动断开连接时，会抛出异常
//                System.out.println("客户端已断开，连接地址: " + clientSocket.getInetAddress());
//
//                // 关闭与客户端的连接
//                inputStream.close();
//                outputStream.close();
//                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
