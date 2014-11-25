package gammajoins;

import gammaSupport.ThreadList;
import gammaSupport.Tuple;
import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;

public class Sink extends Thread {

	ReadEnd read;
	public Sink(Connector read){
		this.read = read.getReadEnd();
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
