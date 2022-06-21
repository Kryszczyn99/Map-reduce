import java.net.Socket;

public class WorkerInfo {

    private Socket socket;
    private String job_result_file;
    private boolean does_have_a_job;
    private String job_file;



    private int unique_id_of_job;

    private String job;
    private int index;
    private WorkerHandler wh;

    public WorkerInfo(Socket socket, String worker_file, boolean does_have_a_job, String job, int index, WorkerHandler wh, String j, int unique_id) {
        this.socket = socket;
        this.job_result_file = worker_file;
        this.does_have_a_job = does_have_a_job;
        this.job_file = job;
        this.index = index;
        this.wh = wh;
        this.job = j;
        this.unique_id_of_job = unique_id;
    }
    public void printWorker(){
        System.out.println(
                        "Socket: " + socket +
                        " Worker_file: " + job_result_file +
                        " Job: " + does_have_a_job +
                        " What job: " + job_file +
                        " Index: " + index +
                                " Job: " + job
        );
    }
    public Socket getSocket() {
        return socket;
    }

    public String getJob_result_file() {
        return job_result_file;
    }

    public String getJob_file() {
        return job_file;
    }

    public int getIndex() {
        return index;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public void setJob_result_file(String job_result_file) {
        this.job_result_file = job_result_file;
    }

    public void setJob_file(String job_file) {
        this.job_file = job_file;
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

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }
    public int getUnique_id_of_job() {
        return unique_id_of_job;
    }

    public void setUnique_id_of_job(int unique_id_of_job) {
        this.unique_id_of_job = unique_id_of_job;
    }
}
