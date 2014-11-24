package gammajoins;

import gammaSupport.Tuple;
import basicConnector.ReadEnd;

public class Print extends Thread {
	
	public ReadEnd input;
	public Print(ReadEnd in)
	{
		input = in;
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
