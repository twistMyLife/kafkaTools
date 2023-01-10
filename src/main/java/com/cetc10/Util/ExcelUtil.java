package com.cetc10.Util;

import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ExcelUtil {
    //key保存写入文件名(也就是dto名)，value为文件对象
    private static volatile Map<String,Workbook> singleWorkbook = new ConcurrentHashMap<>();

    public static Workbook getSingleWorkbook(String filepathAndFileName){
        if(!singleWorkbook.containsKey(filepathAndFileName)){
            synchronized (ExcelUtil.class){
                if(!singleWorkbook.containsKey(filepathAndFileName)){
                    if(!isFileExist(filepathAndFileName))
                        creatFile(filepathAndFileName);
                    try {
                        FileInputStream fileInputSream = new FileInputStream(filepathAndFileName);
                        Workbook workbook = new XSSFWorkbook(fileInputSream);
                        singleWorkbook.put(filepathAndFileName,workbook);
                    }catch (Exception e){
                        e.printStackTrace();
                        log.info("新建Excel文件失败");
                    }
                }
            }
        }
        return singleWorkbook.get(filepathAndFileName);
    }

    public static void insert(String filepathAndFileName, ArrayList<String> keyList,ArrayList<String> valueList){
        Workbook workbook = getSingleWorkbook(filepathAndFileName);
        Sheet sheet = workbook.getSheetAt(0);
        if(sheet.getLastRowNum() == 0){
//            插入头
            Row row = sheet.createRow(0);
            for (int i = 0; i < keyList.size(); i++) {
                Cell cell = row.createCell(i);
                cell.setCellValue(keyList.get(i));
            }
        }
        int rowNum = sheet.getLastRowNum()+1;
        Row row = sheet.createRow(rowNum);
        for (int i = 0; i < valueList.size(); i++) {
            Cell cell = row.createCell(i);
            cell.setCellValue(valueList.get(i));
        }
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(filepathAndFileName);
            workbook.write(fileOutputStream);
        }catch (Exception e){
            e.printStackTrace();
            log.info("写入文件失败");
        }
    }

    private static boolean isFileExist(String filepathAndFileName){
        File file = new File(filepathAndFileName);
        return file.exists();
    }

    private static void creatFile(String filepathAndFileName){
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(filepathAndFileName);
            XSSFWorkbook workbook = new XSSFWorkbook();
            workbook.createSheet("Sheet1");
            workbook.write(fileOutputStream);
            workbook.close();
            fileOutputStream.close();
        }catch (Exception e){
            e.printStackTrace();
            log.info("创建文件失败");
        }

    }

    public static void excelInsert(){

    }
}
