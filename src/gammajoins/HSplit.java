package gammajoins;

import gammaSupport.BMap;
import gammaSupport.ReportError;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;
import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;

public class HSplit extends Thread {
	
	ReadEnd tupleIn;
	WriteEnd[] tupleOuts;
	int joinKey;
	public HSplit(Connector tupleIn, Connector tupleOut1, Connector tupleOut2, Connector tupleOut3, Connector tupleOut4, int joinKey) {
		this.tupleIn = tupleIn.getReadEnd();
		this.tupleOuts = new WriteEnd[] {tupleOut1.getWriteEnd(), tupleOut2.getWriteEnd(), tupleOut3.getWriteEnd(), tupleOut4.getWriteEnd()};
		this.tupleOuts[0].setRelation(this.tupleIn.getRelation());
		this.tupleOuts[1].setRelation(this.tupleIn.getRelation());
		this.tupleOuts[2].setRelation(this.tupleIn.getRelation());
		this.tupleOuts[3].setRelation(this.tupleIn.getRelation());
		this.joinKey = joinKey;
		ThreadList.add(this);
	}
	
	public void run () {
		try {
			Tuple tuple;
			while (true) {
				tuple = this.tupleIn.getNextTuple();
				if (tuple == null) {
					break;
				}
				int hash = BMap.myhash(tuple.get(this.joinKey));
				this.tupleOuts[hash].putNextTuple(tuple);
			}
			for (int i = 0; i<BMap.splitLen; i++) {
				this.tupleOuts[i].close();
			}
		} catch (Exception e) {
			ReportError.msg(this, e);
		}
	}
}
