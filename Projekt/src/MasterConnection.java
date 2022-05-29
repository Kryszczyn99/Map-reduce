import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.Socket;
import java.util.*;


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
                else if (parts[0].equals("[REDUCE]") && parts.length > 1) {
                    System.out.println("\n[Worker] Got a reduce function from master.");
                    String fileData = getTextFromFile(path_to_get_job);
                    reduceFunction(path_to_get_job,fileData);
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

        HashMap<String, String> mapDatas = new HashMap<String, String>();
        String[] splittedWords = v.split("[^A-Za-z0-9]+");

        File f = new File(path_for_result_after_job);
        if(!f.exists()) {
            f.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path_for_result_after_job));

        for(String word:splittedWords) {
            mapDatas.put(word, "1");
            JSONObject json = new JSONObject(mapDatas);
            writer.write(json.toString());
            writer.write("\n");
            mapDatas.clear();
        }

        writer.close();
    }
    public void reduceFunction(String k, String v) throws IOException {
        HashMap<String, String> mapDatas = new HashMap<String, String>();
        JSONObject jObject  = new JSONObject(v);
        Iterator<String> keys= jObject.keys();
        while (keys.hasNext())
        {
            int count = 0;
            String keyValue = (String)keys.next();
            JSONArray datas = jObject.getJSONArray(keyValue);
            for(int i = 0; i < datas.length(); i++){
                count += Integer.parseInt(datas.getString(i));
            }
            mapDatas.put(keyValue,String.valueOf(count));
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path_for_result_after_job));
        JSONObject json = new JSONObject(mapDatas);
        writer.write(json.toString());
        writer.close();
        System.out.println(mapDatas);
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