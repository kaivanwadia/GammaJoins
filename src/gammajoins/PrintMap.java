package gammajoins;

import basicConnector.Connector;
import basicConnector.ReadEnd;
import gammaSupport.BMap;
import gammaSupport.ReportError;
import gammaSupport.ThreadList;

public class PrintMap extends Thread {

	ReadEnd in;
	public PrintMap(Connector in){
		this.in = in.getReadEnd();
		ThreadList.add(this);
	}
	
	public void run() {
		try {
			String inputMap = this.in.getNextString();
			System.out.println(inputMap);
		} catch (Exception e) {
			ReportError.msg(this, e);
		}
	}
}
