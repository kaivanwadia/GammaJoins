package gammajoins;

import gammaSupport.BMap;
import gammaSupport.Relation;
import gammaSupport.ReportError;
import gammaSupport.Tuple;
import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;

public class Bloom extends Thread {
	
	ReadEnd in;
	WriteEnd tupleOut;
	WriteEnd bMapOut;
	int joinCol;
	public Bloom (Connector in, Connector tOut, Connector bOut, int joinCol) {
		this.in = in.getReadEnd();
		this.tupleOut = tOut.getWriteEnd();
		this.joinCol = joinCol;
		this.bMapOut = bOut.getWriteEnd();
		this.tupleOut.setRelation(Relation.dummy);
		this.bMapOut.setRelation(Relation.dummy);
	}
	
	public void run() {
		try {
			Tuple tuple;
			String key;
			BMap bMap = BMap.makeBMap();
			while (true) {
				tuple = this.in.getNextTuple();
				if (tuple == null) {
					break;
				}
				key = tuple.get(joinCol);
				bMap.setValue(key, true);
				tupleOut.putNextTuple(tuple);
			}
			tupleOut.close();
			bMapOut.putNextString(bMap.getBloomFilter());
			bMapOut.close();
		} catch (Exception e) {
			ReportError.msg(this, e);
		}
	}
}