import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Master {
    private static final int PORT = 2470;
    private static final ArrayList<WorkerInfo> worker_info_list = new ArrayList<>();
    private static final ExecutorService pool = Executors.newFixedThreadPool(4);
    private static int id = 0;

    private static String path_to_job = "dane_do_przetworzenia";
    private static String path_to_results = "wyniki_danych";

    public static void main(String[] args) throws IOException {
        ServerSocket ss = new ServerSocket(PORT);
        System.out.println("[Master] Master started!");
        ClientAcceptThread clientAccepterThread = new ClientAcceptThread(ss);
        clientAccepterThread.start();
        AliveCheckerThread aliveCheckerThread = new AliveCheckerThread();
        aliveCheckerThread.start();
        while(true)
        {
            Scanner keyboard = new Scanner(System.in);
            String command = keyboard.nextLine();
            System.out.println("Wydana komenda: "+command);
            String[] command_parts = command.split(" ");
            //job polski.txt
            if(command_parts[0].equals("job") && worker_info_list.size()>0){
                String path = path_to_job + "\\" + command_parts[1];
                File file = new File(path);
                BufferedReader br = new BufferedReader(new FileReader(file));
                String st;
                String file_text = "";
                while ((st = br.readLine()) != null){
                    //System.out.println(st);
                    st = st + "\n";
                    file_text = file_text + st;
                }
                int divided = file_text.length()/worker_info_list.size();
                int current_index = 0;
                ArrayList<String> jobs = new ArrayList<>();
                System.out.println(divided);
                for(int i = 0; i < worker_info_list.size(); i++){
                    if(i == worker_info_list.size()){
                        String tp = file_text.substring(current_index,-1);
                        jobs.add(tp);
                        break;
                    }
                    String tp = file_text.substring(current_index,current_index+divided);
                    current_index = current_index + divided;
                    jobs.add(tp);
                }
                for(int i = 0; i < jobs.size(); i++){
                    System.out.println("---------------");
                    System.out.println(jobs.get(i));
                }
                String[] filename_split = command_parts[1].split("\\.");
                int indexes = 1;
                for(int i = 0; i < worker_info_list.size(); i++){
                    String s = String.format("%04d", indexes);
                    indexes += 1;
                    String filename_for_client = filename_split[0] + s + "."+ filename_split[1];
                    String path_for_newfile = path_to_job + "\\" +filename_for_client;
                    File f = new File(path_for_newfile);
                    if(!f.exists()) {
                        f.createNewFile();
                    }
                    String str = jobs.get(i);
                    BufferedWriter writer = new BufferedWriter(new FileWriter(path_for_newfile));
                    writer.write(str);
                    writer.close();

                    String path_for_newfile_result = path_to_results + "\\" + filename_for_client;

                    PrintWriter out = new PrintWriter(worker_info_list.get(i).getSocket().getOutputStream(), true);
                    out.println("[JOB_FILE] " + path_for_newfile);
                    out.println("[RESULT_FILE] " + path_for_newfile_result);
                    out.println("[MAP] test");
                }


            }
            //shuffle dane_do_przetworzenia\polski_result.txt wyniki_danych
            //shuffle dane_do_przetworzenia\polski_result_concat.txt wyniki_danych
            //shuffle     path_to_result_file    path_to_datas_to_shuffle
            else if(command_parts[0].equals("shuffle") && command_parts.length == 3){
                System.out.println("shuffle");

                HashMap<String, ArrayList<String>> mapDatas = new HashMap<String, ArrayList<String>>();

                Set<String> listOfFiles = fileListInDir(command_parts[2]);
                System.out.println(listOfFiles);
                Iterator<String> it = listOfFiles.iterator();

                while(it.hasNext()){
                    String current_file = it.next();
                    String current_file_path = path_to_results + "\\" + current_file;
                    File myObj = new File(current_file_path);
                    Scanner myReader = new Scanner(myObj);
                    while (myReader.hasNextLine()) {
                        String data = myReader.nextLine();
                        JSONObject jObject  = new JSONObject(data);

                        Iterator<String> keys= jObject.keys();
                        while (keys.hasNext())
                        {
                            String keyValue = (String)keys.next();
                            String valueString = jObject.getString(keyValue);
                            mapDatas.computeIfAbsent(keyValue, k -> new ArrayList<>()).add(valueString);
                        }
                    }
                    myReader.close();
                }
                System.out.println(mapDatas);
                File f = new File(command_parts[1]);
                if(!f.exists()) {
                    f.createNewFile();
                }
                BufferedWriter writer = new BufferedWriter(new FileWriter(command_parts[1]));
                JSONObject json = new JSONObject(mapDatas);
                writer.write(json.toString());
                writer.close();

            }
            //reduce polski_result.txt
            else if(command_parts[0].equals("reduce") && worker_info_list.size()>0){
                HashMap<String, ArrayList<String>> mapDatas = new HashMap<String, ArrayList<String>>();

                String path = path_to_job + "\\" + command_parts[1];

                File myObj = new File(path);
                Scanner myReader = new Scanner(myObj);

                while (myReader.hasNextLine()) {

                    String data = myReader.nextLine();
                    JSONObject jObject  = new JSONObject(data);
                    Iterator<String> keys= jObject.keys();
                    while (keys.hasNext())
                    {
                        ArrayList<String> values = new ArrayList<>();
                        String keyValue = (String)keys.next();
                        JSONArray datas = jObject.getJSONArray(keyValue);
                        for(int i = 0; i < datas.length(); i++){
                            values.add(datas.getString(i));
                        }
                        mapDatas.put(keyValue,values);
                    }
                }
                myReader.close();

                //System.out.println(mapDatas);
                //System.out.println(mapDatas.size());


                ArrayList<HashMap<String, ArrayList<String>>> divided_jobs = new ArrayList<HashMap<String, ArrayList<String>>>();


                //System.out.println(returnKeyValueOnIndex(0,mapDatas));

                int divided = mapDatas.size()/worker_info_list.size();
                int current_index = 0;

                for(int i = 0; i < worker_info_list.size(); i++){
                    HashMap<String, ArrayList<String>> temp = new HashMap<String, ArrayList<String>>();
                    //System.out.println("I: "+i+" worker: "+worker_info_list.size());
                    if(i == worker_info_list.size() - 1){

                        for(int j = current_index; j < mapDatas.size(); j++){
                            String key = returnKeyValueOnIndex(j,mapDatas);
                            ArrayList<String> list = mapDatas.get(key);
                            temp.put(key,list);
                        }
                        divided_jobs.add(temp);
                        break;
                    }
                    for(int j = current_index; j < current_index + divided; j++){
                        String key = returnKeyValueOnIndex(j,mapDatas);
                        ArrayList<String> list = mapDatas.get(key);
                        temp.put(key,list);
                    }
                    divided_jobs.add(temp);
                    current_index = current_index + divided;
                }
                //System.out.println(divided_jobs);
                for(int i = 0; i < divided_jobs.size(); i++){
                    System.out.println(divided_jobs.get(i));
                }
                String[] filename_split = command_parts[1].split("\\.");
                int indexes = 1;
                for(int i = 0; i < worker_info_list.size(); i++){
                    String s = String.format("%04d", indexes);
                    indexes += 1;
                    String filename_for_client = filename_split[0] + s + "."+ filename_split[1];
                    String path_for_newfile = path_to_job + "\\" +filename_for_client;
                    File f = new File(path_for_newfile);
                    if(!f.exists()) {
                        f.createNewFile();
                    }
                    HashMap<String, ArrayList<String>> str = divided_jobs.get(i);
                    BufferedWriter writer = new BufferedWriter(new FileWriter(path_for_newfile));
                    JSONObject json = new JSONObject(str);
                    writer.write(json.toString());
                    writer.close();

                    String path_for_newfile_result = path_to_results + "\\" + filename_for_client;

                    PrintWriter out = new PrintWriter(worker_info_list.get(i).getSocket().getOutputStream(), true);
                    out.println("[JOB_FILE] " + path_for_newfile);
                    out.println("[RESULT_FILE] " + path_for_newfile_result);
                    out.println("[REDUCE] test");


                }
            }
        }
    }
    public static String returnKeyValueOnIndex(int index, HashMap<String, ArrayList<String>> mapDatas){
        String key = "";
        int i = 0;
        for(String word:mapDatas.keySet()){
            key = word;
            if(i == index) break;
            i++;
        }
        return key;
    }
    public static Set<String> fileListInDir(String dir) {
        return Stream.of(new File(dir).listFiles())
                .filter(file -> !file.isDirectory())
                .map(File::getName)
                .collect(Collectors.toSet());
    }
    public static class AliveCheckerThread extends Thread {

        public void run(){
            while(true)
            {
                try {
                    Thread.sleep(1000);
                    for(int i = 0; i < worker_info_list.size(); i++){
                        Socket temp = worker_info_list.get(i).getSocket();
                        if(temp.isClosed()){
                            System.out.println("One worker has disconnected!");
                            worker_info_list.remove(i);
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

        }
    }
    public static class ClientAcceptThread extends Thread {
        ServerSocket ss;
        ClientAcceptThread(ServerSocket s){
            ss = s;
        }
        public void run(){
            while(true)
            {
                try {
                    Socket s = ss.accept();
                    System.out.println("[Master] Connected to worker#"+id+"!");
                    WorkerHandler clientThread = new WorkerHandler(s);
                    WorkerInfo temp = new WorkerInfo(s,"",false,"",id, clientThread);
                    worker_info_list.add(temp);
                    id++;
                    pool.execute(clientThread);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}