import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class TCPClient {

    public static void main(String[] args) {
        String serverIp = "127.0.0.1"; // 服务器的IP地址
        int serverPort = 8080; // 服务器的端口号

        try {
            Socket socket = new Socket(serverIp, serverPort);
            System.out.println("已连接到服务器：" + serverIp + ":" + serverPort);

            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();

            // 在这里发送请求并接收服务器的响应

            // 关闭与服务器的连接
            inputStream.close();
            outputStream.close();
            socket.close();
            System.out.println("已断开与服务器的连接");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
