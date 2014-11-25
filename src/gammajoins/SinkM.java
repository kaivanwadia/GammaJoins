package gammajoins;

import gammaSupport.ReportError;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;
import basicConnector.Connector;
import basicConnector.ReadEnd;

public class SinkM extends Thread {
	ReadEnd read;
	public SinkM(Connector read){
		this.read = read.getReadEnd();
		ThreadList.add(this);
	}
	
	public void run()
	{
		try {
			String bMap = this.read.getNextString();
		} catch (Exception e) {
			ReportError.msg(this, e);
		}
		
	}
}
