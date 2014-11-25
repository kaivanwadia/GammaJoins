package gammajoins;

import gammaSupport.BMap;
import gammaSupport.ReportError;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;
import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;

public class BloomFilter extends Thread {
	
	ReadEnd tupleIn;
	ReadEnd bMapIn;
	WriteEnd tupleOut;
	int joinCol;
	public BloomFilter(Connector tupleIn, Connector bMapIn, Connector tupleOut, int joinCol) {
		this.tupleIn = tupleIn.getReadEnd();
		this.bMapIn = bMapIn.getReadEnd();
		this.tupleOut = tupleOut.getWriteEnd();
		this.tupleOut.setRelation(this.tupleIn.getRelation());
		this.joinCol = joinCol;
		ThreadList.add(this);
	}
	
	public void run() {
		try {
			String bMapSerialized = this.bMapIn.getNextString();
			BMap bMap = BMap.makeBMap(bMapSerialized);
			Tuple bTuple;
			while (true) {
				bTuple = this.tupleIn.getNextTuple();
				if (bTuple == null) {
					break;
				}
				if (bMap.getValue(bTuple.get(this.joinCol))) {
					this.tupleOut.putNextTuple(bTuple);
				}
			}
			this.tupleOut.close();
		} catch (Exception e) {
			ReportError.msg(this, e);
		}
	}
}
