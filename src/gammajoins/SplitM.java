package gammajoins;

import gammaSupport.BMap;
import gammaSupport.Relation;
import gammaSupport.ReportError;
import gammaSupport.ThreadList;
import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;

public class SplitM extends Thread {
	
	ReadEnd mapIn;
	WriteEnd[] mapOuts;
	public SplitM(Connector mapIn, Connector mapOut1, Connector mapOut2, Connector mapOut3, Connector mapOut4) {
		this.mapIn = mapIn.getReadEnd();
		this.mapOuts = new WriteEnd[] {mapOut1.getWriteEnd(), mapOut2.getWriteEnd(), mapOut3.getWriteEnd(), mapOut4.getWriteEnd()};
		this.mapOuts[0].setRelation(Relation.dummy);
		this.mapOuts[1].setRelation(Relation.dummy);
		this.mapOuts[2].setRelation(Relation.dummy);
		this.mapOuts[3].setRelation(Relation.dummy);
		ThreadList.add(this);
	}
	
	public void run () {
		try {
			String bMap = this.mapIn.getNextString();
			for (int i = 0; i < BMap.splitLen; i++) {
				String bMapOut = BMap.mask(bMap, i);
				this.mapOuts[i].putNextString(bMapOut);
			}
			this.mapOuts[0].close();
			this.mapOuts[1].close();
			this.mapOuts[2].close();
			this.mapOuts[3].close();
		} catch (Exception e) {
			ReportError.msg(this, e);
		}
	}
}
