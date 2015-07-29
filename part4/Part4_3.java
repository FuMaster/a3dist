import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.hadoop.io.WritableComparable;

import java.util.List;
import java.util.ArrayList;

public class Part4_3 extends EvalFunc<String>{
	TupleFactory mTF = TupleFactory.getInstance();
	BagFactory mBF = BagFactory.getInstance();
	List<String> list = new ArrayList<String>();

	public String exec(Tuple tuple) throws IOException {
		try{
			String str = tuple.toDelimitedString(",");
			String str2 = str.replaceAll( "[\\(\\{\\)]", "");
			str2 = str2.substring(0, str2.length()-1);

			String[] tokens = str2.split("},");

			double sum = 0.0;

			String[] sample1 = tokens[0].split(",");
			String[] sample2 = tokens[1].split(",");

			for(int i=1; i < sample2.length; i++) {
				double v1 = Double.parseDouble(sample1[i]);
				double v2 = Double.parseDouble(sample2[i]);

				sum += v1*v2;
			}

			return sample1[0]+","+sample2[0]+","+sum;
		} catch(Exception e) {
			throw e;
		}
	}
}
