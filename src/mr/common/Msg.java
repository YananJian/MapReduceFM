package mr.common;

import java.util.concurrent.Future;

import mr.common.Constants.JOB_STATUS;
import mr.common.Constants.MSG_TP;
import mr.common.Constants.TASK_STATUS;
import mr.common.Constants.TASK_TP;


public class Msg implements java.io.Serializable{
    private MSG_TP msg_tp = null;
    private TASK_TP task_tp = null;
    private Object content = null;
    private String job_id = null; 
    private String task_id = null;
    private JOB_STATUS job_stat = null;
    private TASK_STATUS task_stat = null;
    private String blk_fpath = null;
    private String machine_id = null;
    private Integer aval_procs = -1;
    private Future future = null;
    private String output_path = null;

    /**
     * get message type, message type includes START_MAPPER, TERMINATE_MAPPER, HEARTBEAT
     * @return
     */
    public MSG_TP getMsg_tp() {
        return msg_tp;
    }

    /**
     * This system uses java concurrent.ExecutorService to run multiple threads, and uses 
     * Future to get the return value of thread.
     * @param future
     */
    public void set_future(Future future)
    {
        this.future = future;
    }

    /**
     * get Future Object, thus to get return value from thread
     * @return
     */
    public Future get_future()
    {
        return this.future;
    }

    /**
     * set Msg type, msg type includes START_MAPPER, TERMINATE_MAPPER, HEARTBEAT
     * @param msg_tp
     */
    public void setMsg_tp(MSG_TP msg_tp) {
        this.msg_tp = msg_tp;
    }

    /**
     * get content within the msg
     * @return
     */
    public Object getContent() {
        return content;
    }

    /**
     * set content to the msg
     * @param content
     */
    public void setContent(Object content) {
        this.content = content;
    }

    /**
     * get jobID
     * @return
     */
    public String getJob_id() {
        return job_id;
    }

    /**
     * set jobID
     * @param job_id
     */
    public void setJob_id(String job_id) {
        this.job_id = job_id;
    }

    /**
     * get job status, job status includes RUNNING, TERMINATED, FINISHED
     * @return
     */
    public JOB_STATUS getJob_stat() {
        return job_stat;
    }

    /**
     * set job status, job status includes RUNNING, TERMINATED, FINISHED
     * @param job_stat
     */
    public void setJob_stat(JOB_STATUS job_stat) {
        this.job_stat = job_stat;
    }

    /**
     * get block path
     * @return
     */
    public String getBlk_fpath() {
        return blk_fpath;
    }

    /**
     * set block path
     * @param blk_fpath
     */
    public void setBlk_fpath(String blk_fpath) {
        this.blk_fpath = blk_fpath;
    }

    /**
     * get task ID
     * @return
     */
    public String getTask_id() {
        return task_id;
    }

    /**
     * set task ID
     * @param task_id
     */
    public void setTask_id(String task_id) {
        this.task_id = task_id;
    }

    /**
     * get task type, task tp includes MAPPER, REDUCER
     * @return
     */
    public TASK_TP getTask_tp() {
        return task_tp;
    }

    /**
     * set task type, task type includes MAPPER, REDUCER
     * @param task_tp
     */
    public void setTask_tp(TASK_TP task_tp) {
        this.task_tp = task_tp;
    }

    /**
     * get task status, task status includes RUNNING, FINISHED, ERROR, TERMINATED
     * @return
     */
    public TASK_STATUS getTask_stat() {
        return task_stat;
    }

    /**
     * set task status, task status incldues RUNNING, FINISHED, ERROR, TERMINATED
     * @param task_stat
     */
    public void setTask_stat(TASK_STATUS task_stat) {
        this.task_stat = task_stat;
    }

    /**
     * get machineID
     * @return
     */
    public String getMachine_id() {
        return machine_id;
    }

    /**
     * set machineID
     * @param machine_id
     */
    public void setMachine_id(String machine_id) {
        this.machine_id = machine_id;
    }

    /**
     * set available process numbers
     * @param num
     */
    public void set_aval_procs(Integer num)
    {
        this.aval_procs = num;
    }

    /**
     * get available process numbers
     * @return
     */
    public Integer get_aval_procs()
    {
        return this.aval_procs;
    }

    /**
     * get output path
     * @return
     */
    public String getOutput_path() {
        return output_path;
    }

    /**
     * set output path
     * @param output_path
     */
    public void setOutput_path(String output_path) {
        this.output_path = output_path;
    }
}
