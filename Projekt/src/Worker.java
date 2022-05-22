import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
public class Worker {
    static final int SERVER_PORT = 2470;
    static final String SERVER_IP = "localhost";
    static Socket s;

    public static void main(String[] args) throws IOException, InterruptedException
    {
        System.out.println("[Client] Waiting for server connection...");

        while (true)
        {
            try {
                s = new Socket(SERVER_IP, SERVER_PORT);
                break;
            } catch (ConnectException e) {
                Thread.sleep(1000);
            }
        }
        System.out.println("[Client] Connected to server.");

        //taking messages from server
        MasterConnection serverConn = new MasterConnection(s);

        BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));
        PrintWriter out = new PrintWriter(s.getOutputStream(), true);

        new Thread(serverConn).start();
        String message = null;
        System.out.println("[Client] Type a notification:");
        while(true)
        {
            System.out.print("> ");
            message = keyboard.readLine();
            out.println(message);
            if (message.equals("quit")) break;
        }

        out.close();
        s.close();
        System.exit(0);
    }
}