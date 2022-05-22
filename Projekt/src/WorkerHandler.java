import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.io.IOException;
import java.io.InputStreamReader;

public class WorkerHandler implements Runnable {
    Socket client;
    BufferedReader in;
    PrintWriter out;

    public WorkerHandler(Socket clientSocket) throws IOException
    {
        client = clientSocket;
        in = new BufferedReader(new InputStreamReader(client.getInputStream()));
        out = new PrintWriter(client.getOutputStream(), true);
    }

    public void run() {
        try {

            while(true)
            {
                String request = in.readLine();
                if (request == null) break;
                if (request.equals("quit"))
                {
                    out.println("quit");
                    System.out.println("[Server] Session closed due to client's request.");
                    break;
                }
                else
                {
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    out.println(request);
                    System.out.println("\n[Server] Request sent !");
                }
            }
        } catch (IOException e) {
            System.err.println("IO exception in client handler (catch)");
        } finally {

            out.close();
            try {
                in.close();
            } catch (IOException e) {
                System.err.println("IO exception in client handler (finally)");
            }
        }
    }
}