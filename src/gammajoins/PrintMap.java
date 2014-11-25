package gammajoins;

import basicConnector.ReadEnd;
import gammaSupport.BMap;
import gammaSupport.ThreadList;

public class PrintMap extends Thread {

	ReadEnd in;
	public PrintMap(ReadEnd in){
		this.in = in;
		ThreadList.add(this);
	}
	
	public void run() {
		try {
			String inputMap = this.in.getNextString();
			System.out.println(inputMap);
		} catch (Exception e) {
			
		}
	}
}
