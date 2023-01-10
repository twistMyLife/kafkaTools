package com.cetc10.domain.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.poi.ss.formula.functions.T;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HttpResponse<T> {
    Integer code;
    String message;
    T data;
}
