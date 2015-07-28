import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.hadoop.io.WritableComparable;
import java.util.ArrayList;
import java.util.List;

public class Part4_2 extends EvalFunc<DataBag>{
  private List<String> list = new ArrayList<String>();

  public DataBag exec(Tuple tuple) throws IOException {
    try{
      String str = tuple.toDelimitedString(",");
      String str2 = str.replaceAll("\\(","");
      String str3 = str2.replaceAll("\\)","");
      String[] tokens = str3.split(",");
      
      double max = 0;
      int index = 1;
      DataBag result = BagFactory.getInstance().newDefaultBag();
      for(int i=0;i<tokens.length;i++)
      {	
        list.add("gene_"+index);
        list.add(tokens[i]);
        Tuple t = TupleFactory.getInstance().newTuple(list);
        result.add(t);
        list.clear();
      	index++;
      }
      return result;
    }
    catch(Exception e)
    {
    	throw e;
    }
  }
}
