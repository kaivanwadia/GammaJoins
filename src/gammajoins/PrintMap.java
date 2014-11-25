package gammajoins;

import gammaSupport.BMap;
import gammaSupport.ThreadList;

public class PrintMap extends Thread {

	BMap map;
	public PrintMap(BMap map){
		this.map = map;
		ThreadList.add(this);
	}
	
	public void run(){
		
	}
}
