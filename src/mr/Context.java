package mr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import mr.common.Constants.TASK_TP;
import mr.io.Writable;
import mr.io.TextWritable;

public class Context {

	private String job_id = null;
	private String task_id = null; //mapper id or reducer id
	private HashMap<String, Integer> idSize = new HashMap<String, Integer>();
	private int reducer_ct = 0;
	private String dir = "";
	private LinkedList<Record> contents = new LinkedList<Record>();
    private int numBuffers = 0;
    private String bufferPathPrefix = "";
	private TASK_TP task_tp = null;
    private final int kBufferSize = 1000;
    
    
	public Context(String job_id, String task_id, int reducer_ct, String dir, TASK_TP tp)
	{
		this.job_id = job_id;
		this.task_id = task_id;
		this.reducer_ct = reducer_ct;
		this.dir = dir;
        this.bufferPathPrefix = "/tmp/" + task_id + "tmp/";
        this.task_tp = tp;
	}
	
	public HashMap<String, Integer> get_idSize()
	{
		return this.idSize;
	}
	
	public String get_JobID()
	{
		return this.job_id;
	}
	
	public String get_TaskID()
	{
		return this.task_id;
	}
	
	public BufferedReader getContents() throws IOException
	{
        /* flush remaining contents */
        if (!contents.isEmpty())
            writeBuffer();
        if (numBuffers > 0)
            return new BufferedReader(new FileReader(bufferPathPrefix + "0"));
        else
            return null;
	}
	
	protected void partition() throws IOException
	{
        File dirFile = new File(dir);
        if (!dirFile.exists())
            dirFile.mkdirs();
        /* flush remaining contents */
        if (!contents.isEmpty())
            writeBuffer();
        /* open buffer files */
        HashMap<Integer, BufferedReader> bufferFiles= new HashMap<Integer, BufferedReader>();
        for (int i = 0; i < numBuffers; i++)
            bufferFiles.put(i, new BufferedReader(new FileReader(bufferPathPrefix + i)));
        /* open BufferWriters for partition files */
        HashMap<Integer, BufferedWriter> partitionFiles= new HashMap<Integer, BufferedWriter>();
        for (int i = 0; i < reducer_ct; i++)
            partitionFiles.put(i, new BufferedWriter(new FileWriter(dir + task_id + '@' + i)));
        /* insert one record/file into records */
        LinkedList<Record> records = new LinkedList<Record>();
        for (int i = 0; i < numBuffers; i++) {
            String line = bufferFiles.get(i).readLine();
            if (line != null) {
                String[] tokens = line.split("\t");
                TextWritable key = new TextWritable();
                TextWritable value = new TextWritable();
                key.setVal(tokens[0]);
                value.setVal(tokens[1]);
                Record record = new Record(key, tokens[2]);
                record.addValue(value);
                records.add(record);
            }
        }
        /* sort the list */
        Collections.sort(records);
        /* begin n-way merge sort */
        while (!records.isEmpty()) {
            Record record = records.poll();
            String key = (String) record.getKey().getVal();
            String value = (String) record.getValues().iterator().next().getVal();
            String bufferId = record.getFileName();
            int partitionId = key.hashCode() % reducer_ct;
            String path = dir + task_id + '@' + String.valueOf(partitionId);
            String str = key + "\t" + value + "\n";
            partitionFiles.get(partitionId).write(str);
            System.out.println("Writing to disk, path: " + path + "\tcontent: " + str);
            /* get the next record from buffer */
            String line = bufferFiles.get(Integer.parseInt(bufferId)).readLine();
            if (line != null) {
                String[] tokens = line.split("\t");
                TextWritable k = new TextWritable();
                TextWritable v = new TextWritable();
                k.setVal(tokens[0]);
                v.setVal(tokens[1]);
                Record r = new Record(k, tokens[2]);
                r.addValue(v);
                records.add(r);
                Collections.sort(records);
            }
            /* resolve idSize */
            Integer nLines = idSize.get(partitionId);
            if (nLines == null)
                idSize.put(String.valueOf(partitionId), 1);
            else
                idSize.put(String.valueOf(partitionId), nLines+1);
        }
        /* close all files */
        for (int i = 0; i < numBuffers; i++)
            bufferFiles.get(i).close();
        for (int i = 0; i < reducer_ct; i++)
            partitionFiles.get(i).close();
	}
	
	public void write(Writable key, Writable value)
	{
        Record record = new Record(key, String.valueOf(numBuffers));
        record.addValue(value);
		contents.add(record);		

        if (contents.size() >= kBufferSize)
            /* buffer is full, dump to tmp file */
            writeBuffer();
	}

    private void writeBuffer()
    {
        /* sort records */
        Collections.sort(contents);
        File pathFile = new File(bufferPathPrefix);
        if (!pathFile.exists())
            pathFile.mkdirs();
        try {
            File file = new File(bufferPathPrefix + numBuffers);
            if (task_tp == TASK_TP.MAPPER)
                numBuffers++;
            BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
            for (Record r : contents) {
                bw.write(r.getKey().getVal() + "\t" + r.getValues().iterator().next().getVal());
                if (task_tp == TASK_TP.MAPPER)
                    bw.write("\t" + r.getFileName());
                bw.write("\n");
            }
            bw.close();
            contents.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
