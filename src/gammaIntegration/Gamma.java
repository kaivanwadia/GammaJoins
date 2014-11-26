package gammaIntegration;

import basicConnector.Connector;
import gammaSupport.ArrayConnectors;
import gammajoins.Bloom;
import gammajoins.BloomFilter;
import gammajoins.HJoin;
import gammajoins.HSplit;
import gammajoins.Merge;

public class Gamma extends ArrayConnectors{
	public Gamma (Connector aTuplesIn, Connector bTuplesIn, int aJoinKey, int bJoinKey, Connector output) {
		Connector[] hSplitAToBloom = ArrayConnectors.initConnectorArray("hSplitAToBloom");
		Connector[] hSplitBToBFilter = ArrayConnectors.initConnectorArray("hSplitAToBloom");
		Connector[] bloomAToHJoin = ArrayConnectors.initConnectorArray("bloomAToHJoin");
		Connector[] bloomMapToBFilter = ArrayConnectors.initConnectorArray("bloomMapToBFilter");
		Connector[] bFilterToHjoin = ArrayConnectors.initConnectorArray("bFilterToHjoin");
		Connector[] hJoinToMerge = ArrayConnectors.initConnectorArray("hJoinToMerge");
		
		HSplit hSplitA = new HSplit(aTuplesIn, hSplitAToBloom[0], hSplitAToBloom[1], hSplitAToBloom[2], hSplitAToBloom[3], aJoinKey);
		HSplit hSplitB = new HSplit(bTuplesIn, hSplitBToBFilter[0], hSplitBToBFilter[1], hSplitBToBFilter[2], hSplitBToBFilter[3], bJoinKey);
		
		Bloom bloom0 = new Bloom(hSplitAToBloom[0], bloomAToHJoin[0], bloomMapToBFilter[0], aJoinKey);
		Bloom bloom1 = new Bloom(hSplitAToBloom[1], bloomAToHJoin[1], bloomMapToBFilter[1], aJoinKey);
		Bloom bloom2 = new Bloom(hSplitAToBloom[2], bloomAToHJoin[2], bloomMapToBFilter[2], aJoinKey);
		Bloom bloom3 = new Bloom(hSplitAToBloom[3], bloomAToHJoin[3], bloomMapToBFilter[3], aJoinKey);
		
		BloomFilter bFilter0 = new BloomFilter(hSplitBToBFilter[0], bloomMapToBFilter[0], bFilterToHjoin[0], bJoinKey);
		BloomFilter bFilter1 = new BloomFilter(hSplitBToBFilter[1], bloomMapToBFilter[1], bFilterToHjoin[1], bJoinKey);
		BloomFilter bFilter2 = new BloomFilter(hSplitBToBFilter[2], bloomMapToBFilter[2], bFilterToHjoin[2], bJoinKey);
		BloomFilter bFilter3 = new BloomFilter(hSplitBToBFilter[3], bloomMapToBFilter[3], bFilterToHjoin[3], bJoinKey);
		
		HJoin hJoin0 = new HJoin(bloomAToHJoin[0], bFilterToHjoin[0], aJoinKey, bJoinKey, hJoinToMerge[0]);
		HJoin hJoin1 = new HJoin(bloomAToHJoin[1], bFilterToHjoin[1], aJoinKey, bJoinKey, hJoinToMerge[1]);
		HJoin hJoin2 = new HJoin(bloomAToHJoin[2], bFilterToHjoin[2], aJoinKey, bJoinKey, hJoinToMerge[2]);
		HJoin hJoin3 = new HJoin(bloomAToHJoin[3], bFilterToHjoin[3], aJoinKey, bJoinKey, hJoinToMerge[3]);
		
		Merge merge = new Merge(hJoinToMerge[0], hJoinToMerge[1], hJoinToMerge[2], hJoinToMerge[3], output);
	}
}
