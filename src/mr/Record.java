package mr;

import java.util.LinkedList;

import mr.io.Writable;

/**
 * Record stores a (key, Iterable(value)) tuple and a filename
 * @author Yanan Jian
 * @author Erdong Li
 */
public class Record implements Comparable<Record> {
  private Writable key;
  private Iterable<Writable> values;
  private String filename;

  /**
   * Constructor
   * @param key key
   * @param filename filename of the file that contains the record
   */
  public Record(Writable key, String filename)
  {
    this.key = key;
    this.values = new LinkedList<Writable>();
    this.filename = filename;
  }

  /**
   * Get the key
   * @return key of the record
   */
  public Writable getKey()
    { return this.key; }

  /**
   * Add a value to values
   * @param value a new value
   */
  public void addValue(Writable value)
    { ((LinkedList<Writable>)values).add(value); }

  /**
   * Get all values
   * @return an iterable list of all values
   */
  public Iterable<Writable> getValues()
    { return this.values; }

  /**
   * Get the filename
   * @return the filename
   */
  public String getFileName()
    { return this.filename; }

  /**
   * Compare two record
   * @param other the other Record
   */
  @Override
  public int compareTo(Record other)
    { return new Integer(key.getVal().hashCode()).compareTo(new Integer(other.key.getVal().hashCode())); }
}
