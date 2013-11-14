package mr;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.Serializable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Collections;

import mr.io.Writable;
import mr.io.TextWritable;

public abstract class Reducer<K1, V1, K2, V2> implements Serializable {
    private static final long serialVersionUID = 99l;

    private String dirname;
    private HashMap<String, Integer> skipCounts;
    private LinkedList<Record> records;
    
    public void init(String dirname)
    {
        this.dirname = dirname;
        skipCounts = new HashMap<String, Integer>();
        records = new LinkedList<Record>();
    }
    
    public void reduce(TextWritable key, Iterable<Writable> values,
			Context context) {
		
	}

    public void bootstrap() {
        /* insert one record/file into records */
        File dir = new File(dirname);
        File[] files = dir.listFiles();
        for (File file : files) {
            String filename = file.getName();
      
            skipCounts.put(filename, 1);
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = br.readLine();
                if (line != null) {
                    /* skip empty file */
                    String[] words = line.split("\t");
                    /* HARD CODE TYPE TO BE TEXTWRITABLE */
                    TextWritable key = new TextWritable();
                    TextWritable value = new TextWritable();
                    key.setVal(words[0]);
                    value.setVal(words[1]);
                    Record record = new Record(key, filename);
                    record.addValue(value);
                    records.add(record);
                }
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        Collections.sort(records);
    }

    public Record getNext() throws IOException
    {
        if (records.isEmpty())
          return null;
        Writable key = records.getFirst().getKey();
        Record result = new Record(key, null);
        while (!records.isEmpty() && records.getFirst().getKey().getVal().hashCode() == key.getVal().hashCode()) {
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
            if (line != null) {
                String[] words = line.split("\t");
                /* HARD CODE TYPE TO BE TEXTWRITABLE */
                TextWritable k = new TextWritable();
                TextWritable v = new TextWritable();
                k.setVal(words[0]);
                v.setVal(words[1]);
                /* reinsert entry */
                Record tmp = new Record(k, filename);
                tmp.addValue(v);
                records.add(tmp);
                Collections.sort(records);
            }
            br.close();
        }
        return result;
    }

	

	
}
