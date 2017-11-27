package keyValueStore.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class writeLog {
	String fileName = null;
	BufferedWriter bw = null;
	FileWriter fileWrite = null;

	public writeLog(String fileNameIn){  
     
        fileName = fileNameIn;
        
		File targetFile = new File(fileName);
		File subdirectory = targetFile.getParentFile();
		if(subdirectory != null){
		    if(!subdirectory.exists() && !subdirectory.mkdir()){
		        System.err.println(" failed to create new subdirectory ");
		        System.exit(0); 
		    }
		}
	}

    /**
     * Writes results to file
     */
	public void writeToFile(String in){
		try{
				fileWrite = new FileWriter(fileName,true);
				bw = new BufferedWriter(fileWrite);
				bw.write(in);
				bw.newLine();
				bw.flush();
				this.close();
		}
		catch(IOException i){
			System.err.println("write failed");
            System.exit(0);
		}
	}

    /**
     * Closes target file 
     */
    public void close() {
		try{
            bw.close();
		}
		catch(IOException i){
			System.err.println("close failed");
            System.exit(0); 
		}
    }
}
