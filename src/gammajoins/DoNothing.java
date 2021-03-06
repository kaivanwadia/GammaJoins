package gammajoins;

import gammaSupport.ThreadList;
import gammaSupport.Tuple;
import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;

public class DoNothing extends Thread {

	ReadEnd read;
	WriteEnd write;
	public DoNothing(Connector read, Connector write){
		this.read = read.getReadEnd();
		this.write = write.getWriteEnd();
		this.write.setRelation(read.getRelation());
		ThreadList.add(this);
	}
	
	public void run()
	{
		try {
			Tuple tuple = read.getNextTuple();
			while(tuple != null)
			{
				write.putNextTuple(tuple);
				tuple = read.getNextTuple();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
