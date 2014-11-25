package gammajoins;

import gammaSupport.Relation;
import gammaSupport.ReportError;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import basicConnector.WriteEnd;

public class ReadRelation extends Thread {
	
	BufferedReader in;
	WriteEnd out;
	Relation relation;
	public ReadRelation (String fileName, WriteEnd out) {
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String input = in.readLine();
			String[] fields = input.split("\\s+");
			this.relation = new Relation(this.getRelationName(fileName), fields.length);
			for (int i = 0; i < fields.length; i++) {
				relation.addField(fields[i]);
			}
			this.out = out;
			this.out.setRelation(relation);
		} catch (Exception e) {
			ReportError.msg(e.getMessage());
		}
		ThreadList.add(this);
	}
	
	public void run() {
		try {
			String input;
			while (true) {
				input = in.readLine();
				if (input == null) {
					break;
				}
				if (input.startsWith("---")) {
					continue;
				}
				Tuple tuple = Tuple.makeTupleFromFileData(this.relation, input);
				out.putNextTuple(tuple);
			}
			in.close();
		} catch (Exception e) {
			ReportError.msg(this.getClass().getName() + e);
		}
	}
	
	public String getRelationName (String fileName) {
		String[] fileBreaks = fileName.split("/");
		String relationName = fileBreaks[fileBreaks.length-1];
		return relationName.split("\\.")[0];
	}
}
