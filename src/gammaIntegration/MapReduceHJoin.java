package gammaIntegration;

import basicConnector.Connector;
import gammaSupport.ArrayConnectors;
import gammajoins.HJoin;
import gammajoins.HSplit;
import gammajoins.Merge;

public class MapReduceHJoin extends ArrayConnectors {
	
	public MapReduceHJoin (Connector aTuplesIn, Connector bTuplesIn, Connector aJoinbOut, int aJoinKey, int bJoinKey) {
		Connector[] aTuplesToHJoin = ArrayConnectors.initConnectorArray("aToHJoin");
		Connector[] bTuplesToHJoin = ArrayConnectors.initConnectorArray("bToHJoin");
		Connector[] hJoinToMerge = ArrayConnectors.initConnectorArray("HJoinToMerge");
		Connector mergeToOut = new Connector("MergeToOut");
		
		HSplit hSplitaTuples = new HSplit(aTuplesIn, aTuplesToHJoin[0], aTuplesToHJoin[1], aTuplesToHJoin[2], aTuplesToHJoin[3], aJoinKey);
		HSplit hSplitbTuples = new HSplit(bTuplesIn, bTuplesToHJoin[0], bTuplesToHJoin[1], bTuplesToHJoin[2], bTuplesToHJoin[3], bJoinKey);
		
		HJoin hJoin0 = new HJoin(aTuplesToHJoin[0], bTuplesToHJoin[0], aJoinKey, bJoinKey, hJoinToMerge[0]);
		HJoin hJoin1 = new HJoin(aTuplesToHJoin[1], bTuplesToHJoin[1], aJoinKey, bJoinKey, hJoinToMerge[1]);
		HJoin hJoin2 = new HJoin(aTuplesToHJoin[2], bTuplesToHJoin[2], aJoinKey, bJoinKey, hJoinToMerge[2]);
		HJoin hJoin3 = new HJoin(aTuplesToHJoin[3], bTuplesToHJoin[3], aJoinKey, bJoinKey, hJoinToMerge[3]);
		
		Merge merge = new Merge(hJoinToMerge[0], hJoinToMerge[1], hJoinToMerge[2], hJoinToMerge[3], mergeToOut);
	}
}
