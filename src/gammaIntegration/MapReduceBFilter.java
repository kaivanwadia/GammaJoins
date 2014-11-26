package gammaIntegration;

import gammaSupport.ArrayConnectors;
import gammajoins.BloomFilter;
import gammajoins.HSplit;
import gammajoins.Merge;
import gammajoins.SplitM;
import basicConnector.Connector;

public class MapReduceBFilter extends ArrayConnectors {
	
	public MapReduceBFilter(Connector mIn, Connector bIn, int joinKey, Connector bOut){
		Connector[] maps = ArrayConnectors.initConnectorArray("map");
		Connector[] bTuples = ArrayConnectors.initConnectorArray("bTuples");
		
		SplitM splitM = new SplitM(mIn, maps[0], maps[1], maps[2], maps[3]);
		HSplit splitB = new HSplit(bIn, bTuples[0], bTuples[1], bTuples[2], bTuples[3], joinKey);
		
		Connector[] bFilterOut = ArrayConnectors.initConnectorArray("bFilterOut");
		
		BloomFilter bFilter0 = new BloomFilter(bTuples[0], maps[0], bFilterOut[0], joinKey);
		BloomFilter bFilter1 = new BloomFilter(bTuples[1], maps[1], bFilterOut[1], joinKey);
		BloomFilter bFilter2 = new BloomFilter(bTuples[2], maps[2], bFilterOut[2], joinKey);
		BloomFilter bFilter3 = new BloomFilter(bTuples[3], maps[3], bFilterOut[3], joinKey);
		
		Merge merge = new Merge(bFilterOut[0], bFilterOut[1], bFilterOut[2], bFilterOut[3], bOut);
		
		
	}

}
