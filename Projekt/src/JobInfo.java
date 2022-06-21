
public class JobInfo {

    private String path_to_get_job;
    private String path_for_result_after_job;
    private String action;
    private int unique_id_of_job;
    public JobInfo(String p_job, String p_result, String ac, int uniq) {
        path_to_get_job = p_job;
        path_for_result_after_job = p_result;
        action = ac;
        unique_id_of_job = uniq;
    }
    public String getPath_to_get_job() {
        return path_to_get_job;
    }

    public void setPath_to_get_job(String path_to_get_job) {
        this.path_to_get_job = path_to_get_job;
    }

    public String getPath_for_result_after_job() {
        return path_for_result_after_job;
    }

    public void setPath_for_result_after_job(String pt) {
        path_for_result_after_job = pt;
    }
    public String getAction() {
        return action;
    }
    public void setAction(String action) {
        this.action = action;
    }
    public void printInfo(){
        System.out.println(
                "Action: " + action +
                        " Path_JOB: " + path_to_get_job +
                        " Path_Result: " + path_for_result_after_job
        );
    }
    public int getUnique_id_of_job() {
        return unique_id_of_job;
    }

    public void setUnique_id_of_job(int unique_id_of_job) {
        this.unique_id_of_job = unique_id_of_job;
    }
}
