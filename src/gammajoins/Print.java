package gammajoins;

import gammaSupport.Relation;
import gammaSupport.ReportError;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;
import basicConnector.Connector;
import basicConnector.ReadEnd;

public class Print extends Thread {
	
	public ReadEnd input;
	public Print(Connector in)
	{
		input = in.getReadEnd();
		ThreadList.add(this);
	}
	
	public void run() {
		try{
			Relation relation = input.getRelation();
			Tuple tuple = input.getNextTuple();
			int spaces = 22;
			String[] fields = relation.getFieldNames();
			for(int i = 0; i < fields.length; i++){
				int numSpaces = spaces - fields[i].length();
				System.out.print(fields[i] + String.format("%"+numSpaces +"s", ""));
			}
			System.out.println();
			System.out.println();
			while(tuple != null)
			{
				for(int i = 0; i < tuple.getSize(); i++){
					int numSpaces = spaces - tuple.get(i).length();
					System.out.print(tuple.get(i) + String.format("%"+numSpaces +"s", ""));
				}
				System.out.println();
				tuple = input.getNextTuple();
			}
		}catch (Exception e)
		{
			ReportError.msg(this, e);
		}
	}

}
