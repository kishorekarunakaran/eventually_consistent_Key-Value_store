package keyValueStore.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
* 
* @author  Surendra kumar Koneti
* @since   2017-11-21
* Description: This program writes the contents to the file specified.
*/

public class writeLog {
	String fileName = null;
	BufferedWriter bw = null;
	FileWriter fileWrite = null;
	
	/**
	 * @param fileNameIn name of the file to write
	 */
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
     * This method writes results to file.
     * @param fileIn line to be written to the file 
     */
	public void writeToFile(String lineIn){
		try{
			fileWrite = new FileWriter(fileName,true);
			bw = new BufferedWriter(fileWrite);
			bw.write(lineIn);
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
     * This method closes the target file. 
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
