package gammajoins;

import gammaSupport.BMap;
import gammaSupport.Relation;
import gammaSupport.ReportError;
import gammaSupport.ThreadList;
import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;

public class MergeM extends Thread {
	ReadEnd[] mapIns;
	WriteEnd mapOut;
	public MergeM(Connector mapIn1, Connector mapIn2, Connector mapIn3, Connector mapIn4, Connector mapOut) {
		this.mapIns = new ReadEnd[] {mapIn1.getReadEnd(), mapIn2.getReadEnd(), mapIn3.getReadEnd(), mapIn4.getReadEnd()};
		this.mapOut = mapOut.getWriteEnd();
		this.mapOut.setRelation(Relation.dummy);
		ThreadList.add(this);
	}
	
	public void run () {
		try {
			BMap finalBMap = BMap.makeBMap(this.mapIns[0].getNextString());
			for (int i = 1; i < 4; i++) {
				BMap tBmap = BMap.makeBMap(this.mapIns[i].getNextString());
				finalBMap = BMap.or(finalBMap, tBmap);
			}
			this.mapOut.putNextString(finalBMap.getBloomFilter());
			this.mapOut.close();
		} catch (Exception e) {
			ReportError.msg(this, e);
		}
	}
}
