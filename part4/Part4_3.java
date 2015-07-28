import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.hadoop.io.WritableComparable;

public class Part4_3 extends EvalFunc<String>{
	public String exec(Tuple tuple) throws IOException {
		try{
			String str = tuple.toDelimitedString(",");
			String str2 = str.replaceAll("\\(","");
			String str3 = str2.replaceAll("\\)","");
			String[] tokens = str3.split(",");

			double max = 0;
			String result = "";
			int index = 1;
			for(int i=0;i<tokens.length;i++)
			{	
				double geneVal = Double.parseDouble(tokens[i]);
				if (geneVal > max)	{
					result = "gene_"+index;
					max = geneVal;
				} else if(geneVal == max) {
					result += ",gene_"+index;
				}
				index++;
			}
			return result;
		} catch(Exception e) {
			throw e;
		}
	}
}
