package com.cecdata.bdp2hive.sqoop.util;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Color;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;

/**
 * Created by zhuweilin on 2017/11/14.
 */
public class POIUtils {

    public static String exetractCellValue(XSSFCell cell){
        if(cell == null){
            return null;
        }
        // 判断不同的单元格类型获取相关类型数据并且转成string类型
        String cellValue = null;
        switch (cell.getCellType()) {
            case HSSFCell.CELL_TYPE_NUMERIC:
                cellValue = String.valueOf((int)cell.getNumericCellValue());
                break;
            case HSSFCell.CELL_TYPE_STRING:
                cellValue = cell.getStringCellValue().trim();
                break;
            case HSSFCell.CELL_TYPE_BOOLEAN:
                cellValue = String.valueOf(cell.getBooleanCellValue());
                break;
            case HSSFCell.CELL_TYPE_FORMULA: // 公式
                cellValue = String.valueOf(cell.getCellFormula());
                break;
            case HSSFCell.CELL_TYPE_BLANK:
                cellValue = null;
                break;
            case HSSFCell.CELL_TYPE_ERROR:
                cellValue = null;
                break;
            default:
                cellValue = null;
                break;
        }
        return cellValue;
    }

    public static String exetractCellValue(Cell cell){
        if(cell == null){
            return null;
        }
        // 判断不同的单元格类型获取相关类型数据并且转成string类型
        String cellValue = null;
        switch (cell.getCellType()) {
            case HSSFCell.CELL_TYPE_NUMERIC:
                cellValue = String.valueOf((int)cell.getNumericCellValue());
                break;
            case HSSFCell.CELL_TYPE_STRING:
                cellValue = cell.getStringCellValue().trim();
                break;
            case HSSFCell.CELL_TYPE_BOOLEAN:
                cellValue = String.valueOf(cell.getBooleanCellValue());
                break;
            case HSSFCell.CELL_TYPE_FORMULA: // 公式
                cellValue = String.valueOf(cell.getCellFormula());
                break;
            case HSSFCell.CELL_TYPE_BLANK:
                cellValue = null;
                break;
            case HSSFCell.CELL_TYPE_ERROR:
                cellValue = null;
                break;
            default:
                cellValue = null;
                break;
        }
        return cellValue;
    }

    public static boolean isRowEmpty1(Row row) {
        for (int c = row.getFirstCellNum(); c < row.getLastCellNum(); c++) {
            Cell cell = row.getCell(c);
            if (cell != null && cell.getCellType() != Cell.CELL_TYPE_BLANK) {
                return false;
            }
        }
        return true;
    }

    public static String getCellStyleColor(XSSFCell cell) {
        String rgb = null;
        if(cell != null){
            XSSFCellStyle cellStyle = cell.getCellStyle();
            if (cellStyle.getFillPattern() == CellStyle.SOLID_FOREGROUND) {
                Color color = cellStyle.getFillForegroundColorColor();
                if (color == null) {
                    return null;
                }
                if (color instanceof XSSFColor) {// .xlsx
                    XSSFColor xc = (XSSFColor) color;
                    rgb = xc.getARGBHex();
                } else if (color instanceof HSSFColor) {// .xls
                    HSSFColor hc = (HSSFColor) color;
                    rgb = hc.getHexString();
                }
            }
        }
        return rgb;
    }

    public static boolean isRowEmpty(Row row) {
        int count = 0;
        for (int c = row.getFirstCellNum(); c < row.getLastCellNum(); c++) {
            Cell cell = row.getCell(c);
            if (cell != null && cell.getCellType() != Cell.CELL_TYPE_BLANK) {
                return false;
            }
        }
        return true;
    }

    public static int countNotEmptyCell(Row row){
        int count = 0;
        for (int c = row.getFirstCellNum(); c < row.getLastCellNum(); c++) {
            Cell cell = row.getCell(c);
            if (cell != null && cell.getCellType() != Cell.CELL_TYPE_BLANK) {
                count++;
            }
        }
        return count;
    }

}
