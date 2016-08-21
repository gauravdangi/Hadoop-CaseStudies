import java.io.*;
import java.util.*;

import org.apache.pig.*;

import org.apache.pig.backend.executionengine.*;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.*;

public class GenerateTitle extends EvalFunc<String>{
	
	public String exec(Tuple input) throws IOException{
		if(input == null || input.size()==0){
			return null;
		}
		String name = (String)input.get(0);
		
		if(name.matches("Mr. ")){
			return("Mr ");
		}
		else if(name.matches("Mrs. ")){
			return("Mrs ");
		}
		else if(name.matches("Master")){
			return("Master");
		}
		else if(name.matches("Miss. ")){
			return("Miss ");
		}
		else {
			return("Unknown");
		}
		
		
	}

}
