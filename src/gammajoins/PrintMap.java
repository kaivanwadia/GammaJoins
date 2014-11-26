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
			for(int i = 0; i < BMap.splitLen; i++){
				for(int j = 0; j < BMap.mapSize; j++){
					System.out.print(inputMap.charAt(i*BMap.mapSize + j));
				}
				System.out.println();
			}
		} catch (Exception e) {
			ReportError.msg(this, e);
		}
	}
}
