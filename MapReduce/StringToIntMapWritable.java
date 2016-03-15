package fr.eurecom.dsg.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int 
 *
 **/
public class StringToIntMapWritable implements Writable {
  
  // TODO: add an internal field that is the real associative array
  private HashMap<String, Integer> map = new HashMap<>();
  //int fakeC;

  @Override
  public void readFields(DataInput in) throws IOException {
    
    // TODO: implement deserialization
    // Warning: for efficiency reasons, Hadoop attempts to re-use old instances of
    // StringToIntMapWritable when reading new records. Remember to initialize your variables 
    // inside this function, in order to get rid of old data.

    map.clear();
    int length = in.readInt();
    for ( int i=0; i<length; i++){
      map.put(in.readUTF(), in.readInt());
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO: implement serialization
    out.writeInt(map.size());
    for(Map.Entry<String,Integer> entry : map.entrySet()){
      out.writeUTF(entry.getKey());
      out.writeInt(entry.getValue());
    }

  }

  public Integer get(String key) {
    return map.get(key);
  }

  public void addValue(String key, Integer value) {
    map.put(key,value);
  } // setter

  public String toString() {
    return map.toString();
  }

  public void clear() {
    map.clear();
  }

  public Set<Map.Entry<String, Integer>> entrySet() {
    return map.entrySet();
  }



}

