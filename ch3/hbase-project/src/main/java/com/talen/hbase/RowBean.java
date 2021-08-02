package com.talen.hbase;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class RowBean {
    private String rowKey;
    private Map<String, List<ColBean>> columnFamilys;
}
