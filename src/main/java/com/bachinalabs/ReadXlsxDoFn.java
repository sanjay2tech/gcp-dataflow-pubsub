package com.bachinalabs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Iterator;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;

public class ReadXlsxDoFn extends DoFn<FileIO.ReadableFile, String>{
    final static String  DELIMITER  = ";";
    @ProcessElement
    public void process(ProcessContext c) throws IOException {

        try{
            FileIO.ReadableFile  fileName = c.element();
            System.out.println("FileName being read is :" + fileName);
            assert fileName != null;
            InputStream stream = Channels.newInputStream(fileName.openSeekable());
            XSSFWorkbook wb = new XSSFWorkbook(stream);
            XSSFSheet sheet = wb.getSheetAt(0);     //creating a Sheet object to retrieve object
            //iterating over Excel file
            for (Row row : sheet) {
                Iterator<Cell> cellIterator = row.cellIterator();   //iterating over each column
                StringBuilder sb  = new StringBuilder();
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
    
                    if(cell.getCellType() ==  CellType.NUMERIC){
                        sb.append(cell.getNumericCellValue()).append(DELIMITER);
                    }
                    else{
                        sb.append(cell.getStringCellValue()).append(DELIMITER);
                    }
                }
                System.out.println(sb.substring(0, sb.length()-1));
                c.output(sb.substring(0, sb.length()-1));//removing the delimiter present @End of String 
            }
            wb.close();
        }
        catch(Exception e) {
            e.printStackTrace();
        }      
    }
}