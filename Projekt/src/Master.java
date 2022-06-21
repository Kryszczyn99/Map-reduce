import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Master {
    private static final int PORT = 2470;
    private static final ArrayList<WorkerInfo> worker_info_list = new ArrayList<>();
    private static final ArrayList<WorkerInfo> job_info_list = new ArrayList<>();
    private static final ExecutorService pool = Executors.newFixedThreadPool(4);
    private static int id = 0;
    private static int id_of_job = 0;
    private static boolean jobDone = true;
    static int cut_value = 3;

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
                jobDone = false;
                String path = path_to_job + "\\" + command_parts[1];
                File file = new File(path);
                FileReader fr = new FileReader(file);
                BufferedReader br = new BufferedReader(fr);
                String st;
                String file_text = "";
                while ((st = br.readLine()) != null){
                    //System.out.println(st);
                    st = st + "\n";
                    file_text = file_text + st;
                }
                br.close();
                fr.close();
                int divided = file_text.length()/cut_value;
                int current_index = 0;
                ArrayList<String> jobs = new ArrayList<>();
                System.out.println(divided);
                for(int i = 0; i < cut_value; i++){
                    if(i == cut_value){
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
                ArrayList<String> send_jobs = new ArrayList<String>();
                String[] filename_split = command_parts[1].split("\\.");
                int indexes = 1;
                for(int i = 0; i < cut_value; i++){
                    String s = String.format("%04d", indexes);
                    indexes += 1;
                    String filename_for_client = filename_split[0] + s + "."+ filename_split[1];
                    String path_for_newfile = path_to_job + "\\" +filename_for_client;
                    File f = new File(path_for_newfile);
                    if(!f.exists()) {
                        f.createNewFile();
                    }
                    String str = jobs.get(i);
                    FileWriter fw = new FileWriter(path_for_newfile);
                    BufferedWriter writer = new BufferedWriter(fw);
                    writer.write(str);
                    writer.close();
                    fw.close();

                    String path_for_newfile_result = path_to_results + "\\" + filename_for_client;

                    //PrintWriter out = new PrintWriter(worker_info_list.get(i).getSocket().getOutputStream(), true);
                    //out.println("[JOB_FILE] " + path_for_newfile);
                    //out.println("[RESULT_FILE] " + path_for_newfile_result);
                    //out.println("[MAP] " + path_for_newfile + " " + path_for_newfile_result);
                    send_jobs.add("[MAP] " + path_for_newfile + " " + path_for_newfile_result);
                }
                System.out.println("Aktywnych workerow: " + worker_info_list.size());
                if(worker_info_list.size()==1 || worker_info_list.size()==2){
                    PrintWriter out = new PrintWriter(worker_info_list.get(0).getSocket().getOutputStream(), true);
                    for(String text : send_jobs){
                        out.println(text + " " + id_of_job);
                        String[] files_spllited = text.split(" ");
                        WorkerInfo temp = new WorkerInfo(
                                worker_info_list.get(0).getSocket(),
                                files_spllited[2],
                                true,
                                files_spllited[1],
                                0,
                                null,
                                "[MAP]",
                                id_of_job
                        );
                        id_of_job++;
                        job_info_list.add(temp);
                    }
                }
                else{
                    int min = 0;
                    int max = worker_info_list.size() - 2;

                    for(String text : send_jobs){
                        int randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
                        PrintWriter out = new PrintWriter(worker_info_list.get(randomNum).getSocket().getOutputStream(), true);
                        out.println(text + " " + id_of_job);
                        String[] files_spllited = text.split(" ");
                        WorkerInfo temp = new WorkerInfo(
                                worker_info_list.get(randomNum).getSocket(),
                                files_spllited[2],
                                true,
                                files_spllited[1],
                                worker_info_list.get(randomNum).getIndex(),
                                null,
                                "[MAP]",
                                id_of_job
                        );
                        id_of_job++;
                        job_info_list.add(temp);
                    }


                }
                for(WorkerInfo t : job_info_list){
                    t.printWorker();
                }
            }
            //shuffle dane_do_przetworzenia\polski_result.txt wyniki_danych
            //shuffle dane_do_przetworzenia\polski_result_concat.txt wyniki_danych

            //USUN PLIKI W WYNIKI_DANYCH PRZED SHUFFLE

            //shuffle     path_to_result_file    path_to_datas_to_shuffle
            else if(command_parts[0].equals("shuffle") && command_parts.length == 3){
                job_info_list.clear();
                System.out.println("shuffle");

                HashMap<String, ArrayList<String>> mapDatas = new HashMap<String, ArrayList<String>>();

                Set<String> listOfFiles = fileListInDir(command_parts[2]);
                System.out.println(listOfFiles);
                if(listOfFiles.size()!=cut_value){
                    System.out.println("Nie skonczono poprzedniego taska!");
                    continue;
                }
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
                FileWriter fw = new FileWriter(command_parts[1]);
                BufferedWriter writer = new BufferedWriter(fw);
                JSONObject json = new JSONObject(mapDatas);
                writer.write(json.toString());
                writer.close();
                fw.close();
                File file = new File(command_parts[2]);
                deleteDirectory(file);
            }
            //reduce polski_result.txt

            else if(command_parts[0].equals("reduce") && worker_info_list.size()>0){
                jobDone = false;
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

                int divided = mapDatas.size()/cut_value;
                int current_index = 0;

                for(int i = 0; i < cut_value; i++){
                    HashMap<String, ArrayList<String>> temp = new HashMap<String, ArrayList<String>>();
                    //System.out.println("I: "+i+" worker: "+worker_info_list.size());
                    if(i == cut_value - 1){

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
                ArrayList<String> send_jobs = new ArrayList<String>();
                String[] filename_split = command_parts[1].split("\\.");
                int indexes = 1;
                for(int i = 0; i < cut_value; i++){
                    String s = String.format("%04d", indexes);
                    indexes += 1;
                    String filename_for_client = filename_split[0] + s + "."+ filename_split[1];
                    String path_for_newfile = path_to_job + "\\" +filename_for_client;
                    File f = new File(path_for_newfile);
                    if(!f.exists()) {
                        f.createNewFile();
                    }
                    HashMap<String, ArrayList<String>> str = divided_jobs.get(i);
                    FileWriter fw = new FileWriter(path_for_newfile);
                    BufferedWriter writer = new BufferedWriter(fw);
                    JSONObject json = new JSONObject(str);
                    writer.write(json.toString());
                    writer.close();

                    String path_for_newfile_result = path_to_results + "\\" + filename_for_client;

                    //PrintWriter out = new PrintWriter(worker_info_list.get(i).getSocket().getOutputStream(), true);
                    //out.println("[JOB_FILE] " + path_for_newfile);
                    //out.println("[RESULT_FILE] " + path_for_newfile_result);
                    send_jobs.add("[REDUCE] " + path_for_newfile + " " + path_for_newfile_result);
                    //out.println("[REDUCE] " + path_for_newfile + " " + path_for_newfile_result);


                }
                System.out.println("Aktywnych workerow: " + worker_info_list.size());
                if(worker_info_list.size()==1 || worker_info_list.size()==2){
                    PrintWriter out = new PrintWriter(worker_info_list.get(0).getSocket().getOutputStream(), true);
                    for(String text : send_jobs){
                        out.println(text + " " + id_of_job);
                        String[] files_spllited = text.split(" ");
                        WorkerInfo temp = new WorkerInfo(
                                worker_info_list.get(0).getSocket(),
                                files_spllited[2],
                                true,
                                files_spllited[1],
                                0,
                                null,
                                "[REDUCE]",
                                id_of_job
                        );
                        id_of_job++;
                        job_info_list.add(temp);
                    }
                }
                else{
                    int min = 0;
                    int max = worker_info_list.size() - 2;

                    for(String text : send_jobs){
                        int randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
                        PrintWriter out = new PrintWriter(worker_info_list.get(randomNum).getSocket().getOutputStream(), true);
                        out.println(text + " " + id_of_job);
                        String[] files_spllited = text.split(" ");
                        WorkerInfo temp = new WorkerInfo(
                                worker_info_list.get(randomNum).getSocket(),
                                files_spllited[2],
                                true,
                                files_spllited[1],
                                worker_info_list.get(randomNum).getIndex(),
                                null,
                                "[REDUCE]",
                                id_of_job
                        );
                        id_of_job++;
                        job_info_list.add(temp);
                    }


                }
                for(WorkerInfo t : job_info_list){
                    t.printWorker();
                }
            }

        }
    }
    public static class ThreadFileChecker extends Thread{

        public void run(){
            while(true){
                Set<String> listOfFiles = fileListInDir(path_to_results);
                if(listOfFiles.size()==cut_value){
                    jobDone = true;
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
    public static void deleteDirectory(File directory) {
        Arrays.stream(Objects.requireNonNull(directory.listFiles()))
                .filter(Predicate.not(File::isDirectory))
                .forEach(File::delete);
    }
    //shuffle dane_do_przetworzenia\polski_result.txt wyniki_danych
    //shuffle dane_do_przetworzenia\polski_result_concat.txt wyniki_danych
    //reduce polski_result.txt
    public static class AliveCheckerThread extends Thread {

        public void run(){
            while(true)
            {
                try {
                    Thread.sleep(1000);
                    for(int i = 0; i < worker_info_list.size(); i++){
                        Socket temp = worker_info_list.get(i).getSocket();
                        if(temp.isClosed()){
                            int index = worker_info_list.get(i).getIndex();
                            System.out.println("Worker[#"+ index +"] has disconnected!");

                            worker_info_list.remove(i);
                            if(jobDone == false){
                                System.out.println("Przekazywanie prac!");
                                if(worker_info_list.size() == 0){
                                    System.out.println("Nie ma workerow!");
                                }
                                else if(worker_info_list.size() == 1){
                                    System.out.println("Ostatni zostal");
                                    ListIterator listItr = job_info_list.listIterator();
                                    while(listItr.hasNext()){
                                        WorkerInfo current_info = (WorkerInfo)listItr.next();
                                        current_info.printWorker();
                                        System.out.println(current_info.getIndex());
                                        PrintWriter out = new PrintWriter(worker_info_list.get(0).getSocket().getOutputStream(), true);
                                        String text = current_info.getJob() + " " + current_info.getJob_file() + " " + current_info.getJob_result_file();
                                        out.println(text + " " + current_info.getUnique_id_of_job());
                                        current_info.setIndex(worker_info_list.get(0).getIndex());

                                    }
                                }
                                else{
                                    System.out.println("jest wiecej");
                                    int min = 0;
                                    int max = worker_info_list.size() - 1;
                                    ListIterator listItr = job_info_list.listIterator();
                                    while(listItr.hasNext()){
                                        WorkerInfo current_info = (WorkerInfo)listItr.next();
                                        if(current_info.getIndex() == index){
                                            int randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
                                            PrintWriter out = new PrintWriter(worker_info_list.get(randomNum).getSocket().getOutputStream(), true);
                                            String text = current_info.getJob() + " " + current_info.getJob_file() + " " + current_info.getJob_result_file();
                                            out.println(text + " " + current_info.getUnique_id_of_job());
                                            current_info.setIndex(worker_info_list.get(randomNum).getIndex());
                                        }

                                    }
                                }
                            }

                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
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
                    ThreadFileChecker t = new ThreadFileChecker();
                    t.start();
                    //ThreadJobDone t = new ThreadJobDone(s);
                    //t.start();
                    WorkerInfo temp = new WorkerInfo(s,"",false,"",id, null,"",-1);
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