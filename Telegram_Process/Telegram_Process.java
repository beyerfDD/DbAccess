import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.stream.Collectors;

/**
 * Telegram_Process
 * read a Telegram_Definition textfile and produce the correspondet
 * data insertions for a database table
 *
 * @author Frank Beyer
 * @version 2018_02_01 1.0.0
 */

public class Telegram_Process {
  
  // global defines
  private static final boolean MYSQL = false;  // for conditional compiling
  // MySQL DB
  //private static final String INS_TGM_NAM = "INSERT INTO `tgm_name` (`tgm-id`, `tgm-des`) VALUES (";
  //private static final String INS_PRV_STR = "INSERT INTO `tgm_struct` (";
  //private static final String INS_PRV_MSK = "INSERT INTO `tgm_mask` (";
  //private static final String INS_COL = "`tgm-id`, `tgm-offs`, `tgm-size`, `tgm-msk`, `tgm-code`, `tgm-des`, `tgm-val`, `tgm-img`) VALUES (";
  // MS_SQL DB
  private static final String INS_TGM_NAM = "INSERT INTO [tgm_name] ([tgm-id], [tgm-des]) VALUES (";
  private static final String INS_PRV_STR = "INSERT INTO [tgm_struct] (";
  private static final String INS_PRV_MSK = "INSERT INTO [tgm_mask] (";
  private static final String INS_COL = "[tgm-id], [tgm-offs], [tgm-size], [tgm-msk], [tgm-code], [tgm-des], [tgm-val], [tgm-img]) VALUES (";
  
  private static final String CS = ", ";
  private static final String HC = "'";
  private static final String ONE = "1";
  private static final String INS_EOL = ");";
  private static final String NULL = "null";  // sql: for no column content -> null
  private static final String TGMK = "Telegram Key";
  private static final String ENDK = "End Key";
  
  // sIns includes the assembled sql-Lines
  ArrayList<String> sIns = new ArrayList<String>();
  // directory that contains the telegram file definitions
  String sPath  = "D:\\ZNT_Works\\communication\\tgm_defs";
  String sSqlMy = "INS_TGM_MySQL.sql";  // file with sql-scripts for insert into MySQL
  String sSqlMs = "INS_TGM_MSSQL.sql";  // file with sql-scripts for insert into MSSQL
  String sTgmNm = "";  // telegram name
  String sTgmId = "";  // telegram id (hex-key)
  int   iTgmOfs =  0;  // telegram offset value

  /**
   * Constructor for objects of class Telegram_Process
   */
  public Telegram_Process() {
    getTgmFiles();  // get all telegram description files
    //--checkedGetFile(sPath + "\\0x048c");  // for testing
    System.out.println("inserts: " + sIns.size());
    String sFsql = "";
    if (MYSQL) sFsql = sSqlMy; else sFsql = sSqlMs;
    try {
      FileWriter fw = new FileWriter(sFsql);
      for (ListIterator li = sIns.listIterator(0); li.hasNext();) {
        //--System.out.println(li.next());
        fw.write(li.next().toString());
        fw.write(System.lineSeparator());  // new line
        fw.flush();
      } fw.close();
    } catch (IOException ex) { ex.printStackTrace(); }
    System.out.println("all done ..");
  }
  
  /**
   * Get the telegram definition files from the given path
   * 
   */
  public void getTgmFiles() { 
    File f = new File(sPath);
    File[] aFiles = f.listFiles();
    if (aFiles != null) {
      for (int i = 0; i < aFiles.length; i++) {
        // read the telegram description file
        checkedGetFile(aFiles[i].getAbsoluteFile().toString());
      }
    }
  }
  
  /**
   * Execute 'getFile()', but don't throw exceptions.
   * If an error occurs, write an error message to the terminal.
   *
   * @param sFile the name of the file to read in
   */
  public void checkedGetFile(String sFile) {
    try {
      getFile(sFile);
    } catch(IOException exc) {
      System.out.println("There was a problem getting this file.");
      System.out.println("The error encountered is:");
      System.out.println(exc);
    }
  }
  
  /**
   * Get the content of the file 'sFile'.
   *
   * @param  sFile The name of the file to get content
   * @throws IOException if the file could not be opened
   */
  public void getFile(String sFile) throws IOException {
    String sLine = "";
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(new File(sFile)));
      if (br != null) {
        // overrun the first file line
        sLine  = br.readLine(); sLine = br.readLine();
        // the second file line include the telegram notation (telegram name)
        sTgmNm = sLine;
        // replace a single apostrophe within a string expression
        sTgmNm = replApos(sTgmNm);
        // the third file line is the start key of definition, overrun
        // the forth file line is the telegram key-number
        sLine  = br.readLine(); sLine = br.readLine();
        while(sLine != null) {
          assembleSql(sLine);  
          sLine = br.readLine();  // next line ..
        } 
      }
    } catch (IOException e) { e.printStackTrace(); }
    finally {
      try { if (br != null) br.close(); } catch (IOException ex) { ex.printStackTrace(); }  
    }
  }

  /**
   * replace a single apostrophe within a string expression
   * 
   * @param s String 
   */
   public String replApos(String s) {
     if (MYSQL)
       return s.replace("'", "\\'");  // replace with backslash mask
     else
       return s.replace("'", "''");   // replace with double apostroph
  }

  /**
   * cover a string in apostroph like 'string'
   * 
   * @param s string to cover
   */
  public String getStrCover(String s) { 
    return HC + s + HC; 
  }
  
  /**
   * assemble the sql string for 'insert' the telegram data into
   * a database table
   * 
   * @param sData a seperate textline form input file
   */
  public void assembleSql(String sData) {
    String[] aCol = sData.split("\\t");  // get the sData columns seperated by tabulators
    String sSQL = "";  // stringholder for the insert sql script
    int iCnt  = aCol.length;  // columns counter
    int iOffs = Integer.parseInt(aCol[0]);
    int iSize = 0;
    boolean lMask = false;   // flag for mask-data
    String sCode = aCol[1];  // the telegram value code (hex, ascii)
    sCode = getStrCover(sCode);
    try { iSize = Integer.parseInt(aCol[2]); } catch (NumberFormatException e) { 
      ;  // in case the content is empty, do nothing it's a mask definition
    }
    String sMask = aCol[3];  // the telgram mask definition
    if (sMask.trim().isEmpty()) {
      sMask = NULL;
    } else {
      sMask = getStrCover(sMask); lMask = true;
    }
    String sDesc = aCol[4]; 
    // replace a single apostrophe within a string expression
    sDesc = replApos(sDesc); sDesc = getStrCover(sDesc);
    String sVal = ""; if (iCnt > 5) sVal = aCol[5];
    if (sVal.trim().isEmpty()) sVal = NULL; else sVal = getStrCover(sVal);
    String sImg = ""; if (iCnt > 6) sImg = aCol[6];
    if (sImg.trim().isEmpty()) sImg = NULL;
    // this line of a telegram definition contains the telegram number (hex-key)
    if (iOffs == 1 && sDesc.contains(TGMK) && iCnt > 4) {
      sTgmId = aCol[5]; sTgmId = getStrCover(sTgmId); 
      sTgmNm = getStrCover(sTgmNm);  // set in method getFile()
      sSQL   = INS_TGM_NAM + sTgmId + CS + sTgmNm + INS_EOL;
      //--System.out.println(sSQL);
    } else {
      sSQL = INS_PRV_STR;
      if (iOffs != iTgmOfs) {  // new telegram data-record
        iTgmOfs = iOffs;
        if (iSize == 0) {      // data-record with follow mask reference
          lMask = true; sMask = ONE;
        } else { if (lMask) sSQL = INS_PRV_MSK; }
      } else { if (lMask) sSQL = INS_PRV_MSK; }
      sSQL+= INS_COL + sTgmId + CS + iOffs + CS + iSize + CS + sMask + CS;
      sSQL+= sCode + CS + sDesc + CS + sVal + CS + sImg + INS_EOL;
    }
    if (!sDesc.contains(ENDK) && !sSQL.trim().isEmpty()) {
      sIns.add(sSQL);  //--System.out.println(sSQL);
    }
  }

}
