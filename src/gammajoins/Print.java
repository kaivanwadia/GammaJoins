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
			Tuple tuple = input.getNextTuple();
			while(tuple != null)
			{
				System.out.println(tuple);
				tuple = input.getNextTuple();
			}
		}catch (Exception e)
		{
			ReportError.msg(this, e);
		}
	}

}
