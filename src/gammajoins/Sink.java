package gammajoins;

import gammaSupport.ThreadList;
import gammaSupport.Tuple;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;

public class Sink extends Thread {

	ReadEnd read;
	public Sink(ReadEnd read){
		this.read = read;
		ThreadList.add(this);
	}
	
	public void run()
	{
		Tuple tuple;
		try {
			tuple = read.getNextTuple();
			while(tuple != null){
				tuple = read.getNextTuple();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
