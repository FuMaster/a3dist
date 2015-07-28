import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.hadoop.io.WritableComparable;

import java.util.List;
import java.util.ArrayList;

public class Part4_3 extends EvalFunc<DataBag>{
	TupleFactory mTF = TupleFactory.getInstance();
	BagFactory mBF = BagFactory.getInstance();
	List<String> list = new ArrayList<String>();

	public DataBag exec(Tuple tuple) throws IOException {
		try{
			String str = tuple.toDelimitedString(",");
			String str2 = str.replaceAll( "\\(", "");
			String str3 = str2.replaceAll( "\\)", "");
			String[] tokens = str3.split(",");

			//String sampleID = tokens[0].split("_")[1];
			String sampleID = tokens[0];

			DataBag output = mBF.newDefaultBag();
			for(int i=1;i<tokens.length;i++) {
				if (tokens[i].equals("0.0")) {
					continue;
				}
				list.add(Integer.toString(i));
				list.add(sampleID+":"+tokens[i]);
				output.add(mTF.newTuple(list));
				list.clear();
			}
			return output;
		} catch(Exception e) {
			throw e;
		}
	}
}
