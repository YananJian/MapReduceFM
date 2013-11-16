package mr;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.Serializable;
import java.io.IOException;
import java.util.PriorityQueue;
import java.util.HashMap;

import mr.io.Writable;
import mr.io.TextWritable;

/**
 * Reducer's class
 * @author Yanan Jian
 * @author Erdong Li
 */
public abstract class Reducer<K1, V1, K2, V2> implements Serializable {
    private static final long serialVersionUID = 99l;

    private String dirname;
    private HashMap<String, Integer> skipCounts;
    private PriorityQueue<Record> records;

    /**
     * initiate reducer, set dir, create priorityQueue for sorting
     * @param dirname
     */
    public void init(String dirname)
    {
        this.dirname = dirname;
        skipCounts = new HashMap<String, Integer>();
        records = new PriorityQueue<Record>();
    }
    /**
     * reduce function for client to override
     * @param key
     * @param values
     * @param context
     */
    public void reduce(TextWritable key, Iterable<Writable> values, Context context) {
    }

    /**
     * read partitioned files into bufferedReader based on hashID
     * put key value pairs into priorityQueue for sorting
     * @param hashID
     */
    public void bootstrap(String hashID) {
        /* insert one record/file into records */
        File dir = new File(dirname);
        File[] files = dir.listFiles();
        for (File file : files) {
            String filename = file.getName();
            String tmp[] = filename.split("@");
            if (tmp.length > 1)
                if(!tmp[1].equals(hashID))
                {
                    continue;
                }
            skipCounts.put(filename, 1);
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = br.readLine();
                if (line != null) {
                    /* skip empty file */
                    String[] tokens = line.split("\t");
                    if (tokens.length < 2)
                        continue;
                    TextWritable key = new TextWritable();
                    TextWritable value = new TextWritable();
                    key.setVal(tokens[0]);
                    value.setVal(tokens[1]);
                    Record record = new Record(key, filename);
                    record.addValue(value);
                    records.offer(record);
                }
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    public Record getNext() throws IOException
    {
        if (records.isEmpty())
          return null;
        Writable key = records.peek().getKey();
        //Writable key = records.getFirst().getKey();
        Record result = new Record(key, null);
        while (!records.isEmpty() && records.peek().getKey().getVal().hashCode() == key.getVal().hashCode()) {
            Record record = records.poll();
            result.addValue((Writable) record.getValues().iterator().next());
            String filename = record.getFileName();
            File file = new File(dirname+filename);
            BufferedReader br = new BufferedReader(new FileReader(file));
            /* fix skipCount */
            int skipCount = skipCounts.get(filename);
            skipCounts.put(filename, skipCount+1);
            /* skip first fileCount lines */
            for (int i = 0; i < skipCount; i++)
                br.readLine();
            String line = br.readLine();
            if (line != null)
                if (!line.trim().equals(""))
            {
                String[] tokens = line.split("\t");
                /* HARD CODE TYPE TO BE TEXTWRITABLE */
                TextWritable k = new TextWritable();
                TextWritable v = new TextWritable();
                k.setVal(tokens[0]);
                v.setVal(tokens[1]);
                /* reinsert entry */
                Record tmp = new Record(k, filename);
                tmp.addValue(v);
                records.offer(tmp);
            }
            br.close();
        }
        return result;
    }
}
