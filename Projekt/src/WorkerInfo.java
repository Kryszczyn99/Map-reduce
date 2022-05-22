import java.net.Socket;

public class WorkerInfo {

    private Socket socket;
    private String worker_file;
    private boolean does_have_a_job;
    private String job;
    private int index;
    private WorkerHandler wh;

    public WorkerInfo(Socket socket, String worker_file, boolean does_have_a_job, String job, int index, WorkerHandler wh) {
        this.socket = socket;
        this.worker_file = worker_file;
        this.does_have_a_job = does_have_a_job;
        this.job = job;
        this.index = index;
        this.wh = wh;
    }
    public void printWorker(){
        System.out.println(
                        "Socket: " + socket +
                        " Worker_file: " + worker_file +
                        " Job: " + does_have_a_job +
                        " What job: " + job +
                        " Index: " + index
        );
    }
    public Socket getSocket() {
        return socket;
    }

    public String getWorker_file() {
        return worker_file;
    }

    public String getJob() {
        return job;
    }

    public int getIndex() {
        return index;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public void setWorker_file(String worker_file) {
        this.worker_file = worker_file;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public void setIndex(int index) {
        this.index = index;
    }
    public boolean isDoes_have_a_job() {
        return does_have_a_job;
    }

    public void setDoes_have_a_job(boolean does_have_a_job) {
        this.does_have_a_job = does_have_a_job;
    }

    public WorkerHandler getWh() {
        return wh;
    }

    public void setWh(WorkerHandler wh) {
        this.wh = wh;
    }

}
