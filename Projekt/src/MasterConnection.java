import org.json.JSONObject;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;



public class MasterConnection implements Runnable {
    public Socket server;
    final BufferedReader in;

    private boolean do_have_job;
    private String path_to_get_job;
    private String path_for_result_after_job;

    public MasterConnection(Socket s) throws IOException
    {
        server = s;
        in = new BufferedReader(new InputStreamReader(server.getInputStream()));
        do_have_job = false;
    }
    public void run() {
        String serverResponse = null;
        try {
            while (true)
            {
                serverResponse = in.readLine();
                if (serverResponse == null) {
                    break;
                }
                String[] parts = serverResponse.split(" ");
                if (serverResponse.equals("quit")) {
                    System.out.println("\n[Worker] Connection closed.");
                    break;
                }
                else if (serverResponse.equals("job")) {
                    System.out.println("\n[Worker] Got a job from master.");
                }
                else if (parts[0].equals("[JOB_FILE]") && parts.length > 1) {
                    System.out.println("\n[Worker] Got a file to work on from master.");
                    path_to_get_job = parts[1];
                    do_have_job = true;
                    System.out.println(path_to_get_job);
                }
                else if (parts[0].equals("[RESULT_FILE]") && parts.length > 1) {
                    System.out.println("\n[Worker] Got a path where to save job from master.");
                    path_for_result_after_job = parts[1];
                    System.out.println(path_for_result_after_job);
                }
                else if (parts[0].equals("[MAP]") && parts.length > 1) {
                    System.out.println("\n[Worker] Got a map function from master.");
                    String fileData = getTextFromFile(path_to_get_job);
                    mapFunction(path_to_get_job,fileData);
                }
                else {
                    System.out.println("\n[Worker] Got a notification from server: " + serverResponse);
                }

            }
        } catch (IOException e ) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public void mapFunction(String k,String v) throws IOException {
        HashMap<String, ArrayList<String>> mapDatas = new HashMap<String, ArrayList<String>>();
        String[] splittedWords = v.split("[^A-Za-z0-9]+");
        for(String word:splittedWords) {
            if (mapDatas.containsKey(word)) {
                mapDatas.get(word).add("1");
            } else {
                mapDatas.put(word, new ArrayList<String>(Arrays.asList("1")));
            }
        }
        System.out.println(mapDatas);
        JSONObject json = new JSONObject(mapDatas);
        System.out.println(json);
        File f = new File(path_for_result_after_job);
        if(!f.exists()) {
            f.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path_for_result_after_job));
        writer.write(json.toString());
        writer.close();
    }
    public String getTextFromFile(String path) throws IOException {
        File file = new File(path);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String st;
        String file_text = "";
        while ((st = br.readLine()) != null){
            st = st + "\n";
            file_text = file_text + st;
        }
        return file_text;
    }
}