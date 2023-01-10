package com.cetc10.domain.vo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FuzzySelect {

    public String fuzzyString;
    public int recordConnectionInfoId;

}
