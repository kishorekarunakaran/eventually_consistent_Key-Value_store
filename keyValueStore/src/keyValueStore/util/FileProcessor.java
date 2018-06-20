package keyValueStore.util;

import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.File;

/**
* 
* @author  Surendra kumar Koneti
* @since   2017-11-21
* Description: This program reads the file and returns line by line to the instance, 
*              checks if the file can be read or not.
*/

public class FileProcessor{

	private	FileReader file = null;
	private BufferedReader fileRead = null;
	private int lineIndex = 0;
	private boolean readable = true;
	
	/**
	 * @param fileName the name of the file to read
	 */
	public FileProcessor(String fileName){
	
		File check = new File(fileName);
		
		if(check.isFile()) {
			try{
				file = new FileReader(fileName);
				fileRead = new BufferedReader(file);
			}
			catch(FileNotFoundException f){
				System.err.println(" file not found - " + fileName);
				System.exit(0); 
			}
		}
		else {
			//sets readable to false if the file does not exist.
			setReadable(false);
		}
	}

	/**
	*  Reads target file and returns line by line.
 	*  @return line	
	*/
	public String readLine(){
		String currentLine;
		try{
			currentLine = fileRead.readLine();
			if(currentLine == null){
				return null;
			}
			lineIndex++;
			return currentLine;
		}
		catch(IOException i){
			System.out.println(" Read Failed ");
		}
		return null;
	}

	/**
	 * This method closes the target file.
	 */
    public void close() {
		try{
       	    	fileRead.close();
		}
		catch(IOException i){
			System.err.println("close failed");
         	System.exit(0); 
		}
    }
    
    /**
     * @return readable tells if the file can be read or not
     */
	public boolean isReadable() {
		return readable;
	}
	
	/**
	 * @param readable sets the state of the file, if its readable or not
	 */
	public void setReadable(boolean readable) {
		this.readable = readable;
	}
}
	
