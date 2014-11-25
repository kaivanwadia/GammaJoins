package gammajoins;

import gammaSupport.ThreadList;
import gammaSupport.Tuple;
import basicConnector.ReadEnd;

public class Print extends Thread {
	
	public ReadEnd input;
	public Print(ReadEnd in)
	{
		input = in;
		ThreadList.add(this);
	}
	
	public void run(){
		try{
			Tuple tuple = input.getNextTuple();
			while(tuple != null)
			{
				System.out.println(tuple);
				tuple = input.getNextTuple();
			}
		}catch (Exception e)
		{
			
		}
	}

}
