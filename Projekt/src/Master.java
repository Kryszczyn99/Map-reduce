import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
            //shuffle     path_to_result_file    path_to_datas_to_shuffle
            else if(command_parts[0].equals("shuffle") && command_parts.length == 3){
                System.out.println("Shuffle");
            }

        }
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