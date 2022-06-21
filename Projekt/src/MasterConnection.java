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
    private static String path_for_result_after_job;
    private ArrayList<JobInfo> info = null;

    private static BufferedWriter wr;

    public MasterConnection(Socket s) throws IOException
    {
        info = new ArrayList<JobInfo>();
        server = s;
        in = new BufferedReader(new InputStreamReader(server.getInputStream()));
        do_have_job = false;
    }
    public void run() {
        ThreadGetInfo th = new ThreadGetInfo();
        th.start();
        while(true){
            try {
                Thread.sleep(8000);
                //System.out.println("watek");
                ListIterator listItr = info.listIterator();
                while(listItr.hasNext()){

                    JobInfo current_info = (JobInfo)listItr.next();
                    current_info.printInfo();
                    if(current_info.getAction().equals("MAP")){
                        path_to_get_job = current_info.getPath_to_get_job();
                        path_for_result_after_job = current_info.getPath_for_result_after_job();
                        FileWriter fw = new FileWriter(path_for_result_after_job);
                        wr = new BufferedWriter(fw);

                        System.out.println("\n[Worker] Got a map function from master.");
                        String fileData = getTextFromFile(path_to_get_job);
                        MapReduceFunctions.mapFunction(path_to_get_job,fileData);

                        wr.close();
                        fw.close();

                        listItr.remove();

                        //Master.setJobAsDone(current_info.getUnique_id_of_job());
                    }
                    else if(current_info.getAction().equals("REDUCE")){
                        path_to_get_job = current_info.getPath_to_get_job();
                        path_for_result_after_job = current_info.getPath_for_result_after_job();
                        FileWriter fw = new FileWriter(path_for_result_after_job);
                        wr = new BufferedWriter(fw);

                        System.out.println("\n[Worker] Got a reduce function from master.");
                        String fileData = getTextFromFile(path_to_get_job);
                        JSONObject jObject  = new JSONObject(fileData);
                        Iterator<String> keys= jObject.keys();
                        while (keys.hasNext())
                        {

                            String keyValue = (String)keys.next();
                            JSONArray datas = jObject.getJSONArray(keyValue);
                            String [] valuesTable = new String[datas.length()];
                            for(int i = 0; i < datas.length(); i++){
                                valuesTable[i] = datas.getString(i);
                            }
                            MapReduceFunctions.reduceFunction(keyValue,valuesTable);
                        }
                        //reduceFunction(path_to_get_job,fileData);

                        wr.close();
                        fw.close();

                        listItr.remove();
                        //Master.setJobAsDone(current_info.getUnique_id_of_job());
                    }


                }
                //System.out.print("\n");
            } catch (InterruptedException e) {
                e.printStackTrace();

            } catch (IOException e ) {
                e.printStackTrace();
            }
        }
    }
    public class ThreadGetInfo extends Thread {
        public void run(){
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
                        System.out.println("\n[Worker] Got a file to work on from master.");
                        path_to_get_job = parts[1];
                        do_have_job = true;
                        System.out.println(path_to_get_job);
                        System.out.println("\n[Worker] Got a path where to save job from master.");
                        path_for_result_after_job = parts[2];
                        System.out.println(path_for_result_after_job);
                        System.out.println(parts[3]);

                        JobInfo curr_info = new JobInfo(path_to_get_job,path_for_result_after_job,"MAP", Integer.parseInt(parts[3]));
                        info.add(curr_info);
/*
                        FileWriter fw = new FileWriter(path_for_result_after_job);
                        wr = new BufferedWriter(fw);

                        System.out.println("\n[Worker] Got a map function from master.");
                        String fileData = getTextFromFile(path_to_get_job);
                        MapReduceFunctions.mapFunction(path_to_get_job,fileData);

                        wr.close();
                        fw.close();


 */

                    }
                    else if (parts[0].equals("[REDUCE]") && parts.length > 1) {
                        System.out.println("\n[Worker] Got a file to work on from master.");
                        path_to_get_job = parts[1];
                        do_have_job = true;
                        System.out.println(path_to_get_job);
                        System.out.println("\n[Worker] Got a path where to save job from master.");
                        path_for_result_after_job = parts[2];
                        System.out.println(path_for_result_after_job);

                        JobInfo curr_info = new JobInfo(path_to_get_job,path_for_result_after_job,"REDUCE",Integer.parseInt(parts[3]));
                        info.add(curr_info);
/*
                        FileWriter fw = new FileWriter(path_for_result_after_job);
                        wr = new BufferedWriter(fw);

                        System.out.println("\n[Worker] Got a reduce function from master.");
                        String fileData = getTextFromFile(path_to_get_job);
                        JSONObject jObject  = new JSONObject(fileData);
                        Iterator<String> keys= jObject.keys();
                        while (keys.hasNext())
                        {

                            String keyValue = (String)keys.next();
                            JSONArray datas = jObject.getJSONArray(keyValue);
                            String [] valuesTable = new String[datas.length()];
                            for(int i = 0; i < datas.length(); i++){
                                valuesTable[i] = datas.getString(i);
                            }
                            MapReduceFunctions.reduceFunction(keyValue,valuesTable);
                        }
                        //reduceFunction(path_to_get_job,fileData);

                        wr.close();
                        fw.close();


 */

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
    }
    public static void emit(String k, String v) throws IOException {
        HashMap<String, String> mapDatas = new HashMap<String, String>();
        File f = new File(path_for_result_after_job);
        if(!f.exists()) {
            f.createNewFile();
        }
        mapDatas.put(k,v);
        JSONObject json = new JSONObject(mapDatas);
        wr.write(json.toString());
        wr.write("\n");
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