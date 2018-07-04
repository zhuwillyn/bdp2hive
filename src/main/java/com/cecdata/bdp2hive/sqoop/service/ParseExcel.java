package com.cecdata.bdp2hive.sqoop.service;

import com.cecdata.bdp2hive.sqoop.util.POIUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author zhuweilin
 * @project transfer-tools
 * @description
 * @mail zhuwillyn@163.com
 * @date 2018/05/07 11:59
 */
public class ParseExcel {

    public static Map<String, Set<String>> parse(File file) throws Exception {
        //获取文件输入流
        FileInputStream fis = new FileInputStream(file);
        // 根据输入流构建workbook对象
        XSSFWorkbook workbook = new XSSFWorkbook(fis);
        XSSFSheet sheet = workbook.getSheetAt(0);
        return parseSheet(sheet);
    }


    private static Map<String, Set<String>> parseSheet(XSSFSheet sheet) throws Exception {
        Map<String, Set<String>> map = new HashMap<String, Set<String>>();
        int numberOfRow = sheet.getPhysicalNumberOfRows();
        int numberOfRows = sheet.getLastRowNum();
        int numberRow = Math.max(numberOfRow, numberOfRows);
        XSSFRow _row = sheet.getRow(0);
        int dbRow=0,structRow=0;
        for (int j = 0; j < _row.getPhysicalNumberOfCells(); j++) {
            XSSFCell cell = _row.getCell(j);
            String cellValue = POIUtils.exetractCellValue(cell);
            if(StringUtils.isNotEmpty(cellValue)){
                if(cellValue.contains("数据库名称")){
                    dbRow = j;
                } else if(cellValue.contains("数据集名称")){
                    structRow = j;
                }

            }
        }
        out:
        for (int i = 1; i <= numberRow; i++) {
            XSSFRow row = sheet.getRow(i);
            if (row == null) {
                continue;
            }
            String db = POIUtils.exetractCellValue(row.getCell(dbRow));
            String struct = POIUtils.exetractCellValue(row.getCell(structRow));
            if(map.containsKey(db)){
                map.get(db).add(struct);
            } else {
                Set<String> set = new HashSet<String>();
                set.add(struct);
                map.put(db, set);
            }
        }
        return map;
    }

}
