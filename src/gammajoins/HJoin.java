package gammajoins;

import gammaSupport.Relation;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;

import java.util.ArrayList;
import java.util.HashMap;

import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;

public class HJoin extends Thread {
	ReadEnd a,b;
	WriteEnd out;
	int aCol, bCol;
	public HJoin(Connector a, Connector b, int aCol, int bCol, Connector out)
	{
		this.a = a.getReadEnd();
		this.b = b.getReadEnd();
		this.out = out.getWriteEnd();
		out.setRelation(Relation.dummy);
		this.aCol = aCol;
		this.bCol = bCol;
		
		ThreadList.add(this);
	}
	
	public void run(){
		HashMap<String,ArrayList<Tuple> > mapA = new HashMap<String, ArrayList<Tuple>>();
		try{
			Tuple tuple = a.getNextTuple();
			while(tuple != null){
				if(mapA.get(tuple.get(aCol)) == null)
				{
					ArrayList<Tuple> list = new ArrayList<Tuple>();
					list.add(tuple);
					mapA.put(tuple.get(aCol), list);
				}else{
					ArrayList<Tuple> list = mapA.get(tuple.get(aCol));
					list.add(tuple);
					mapA.put(tuple.get(aCol), list);
				}
				// Broken right here..........
				tuple = a.getNextTuple();
			}
			System.out.println("Here");
			tuple = b.getNextTuple();
			while(tuple != null){
				if(mapA.get(tuple.get(bCol)) != null){
					ArrayList<Tuple> list = mapA.get(tuple.get(bCol));
					for(int i = 0; i < list.size(); i++){
						out.putNextTuple(Tuple.join(list.get(i), tuple, aCol, bCol));
					}
				}
				tuple = b.getNextTuple();
				System.out.println("Looping here!");
			}
			out.close();
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
		
		
	}

}
