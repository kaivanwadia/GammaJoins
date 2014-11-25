package gammajoins;

import gammaSupport.BMap;
import gammaSupport.ReportError;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;
import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;

public class Merge extends Thread {
	
	static Tuple START = new Tuple(0);
	static Tuple LAST = new Tuple(0);
	ReadEnd[] tupleIns;
	WriteEnd tupleOut;
	Tuple[] inputs;
	public Merge(Connector tupleIn1, Connector tupleIn2, Connector tupleIn3, Connector tupleIn4, Connector tupleOut) {
		this.tupleIns = new ReadEnd[] {tupleIn1.getReadEnd(), tupleIn2.getReadEnd(), tupleIn3.getReadEnd(), tupleIn4.getReadEnd()};
		this.tupleOut = tupleOut.getWriteEnd();
		this.tupleOut.setRelation(this.tupleIns[0].getRelation());
		this.inputs = new Tuple[] {START, START, START, START};
		ThreadList.add(this);
	}
	
	public void run () {
		try {
			this.readInput(0);
			this.readInput(1);
			this.readInput(2);
			this.readInput(3);
			while (true) {
				boolean printing = false;
				for (int i = 0; i < BMap.splitLen; i++) {
					if (this.inputs[i] != LAST) {
						this.tupleOut.putNextTuple(this.inputs[i]);
						this.readInput(i);
						printing = true;
					}
				}
				if (!printing) {
					break;
				}
			}
			this.tupleOut.close();
		} catch (Exception e) {
			ReportError.msg(this, e);
		}
	}
	
	public void readInput (int inputNo) throws Exception {
		if (this.inputs[inputNo] == LAST) {
			return;
		}
		try {
			this.inputs[inputNo] = this.tupleIns[inputNo].getNextTuple();
			if (this.inputs[inputNo] == null) {
				this.inputs[inputNo] = LAST;
			}
		} catch (Exception e) {
			ReportError.msg(e.getMessage());
		}
	}
}
