INSERT INTO CHINA_DEMAND_TEST
SELECT *
FROM
  ( WITH IMC_TYPES_HIST AS
  (SELECT T2.IMC_NO,
    T1.SNAP_MO_YR_KEY_NO,
    CASE
      WHEN T1.IMC_CNTRY_KEY_NO=7
      THEN 50
      WHEN T1.IMC_CNTRY_KEY_NO=36
      THEN 360
    END AS CNTRY_CD,
    CASE
      WHEN TRIM(T1.BUS_NATR_CD) IN ('C', '3', 'V')
      THEN 'RC'
      ELSE 'ABO'
    END AS IMC_TYPE_AGG
  FROM
    (SELECT SNAP_MO_YR_KEY_NO,
      IMC_KEY_NO,
      IMC_CNTRY_KEY_NO,
      BUS_NATR_CD
    FROM DWSAVR02.DWV00050_IMC_CCYYMM_FACT
    WHERE SNAP_MO_YR_KEY_NO=202102
    AND IMC_CNTRY_KEY_NO  IN (36,7)
    ) T1
  LEFT JOIN
    (SELECT IMC_NO,
      IMC_KEY_NO
    FROM DWSAVR02.DWV01021_IMC_MASTER_DIM
    WHERE IMC_AFF_KEY_NO =30
    ) T2 USING(IMC_KEY_NO)
  ),
  ORD_HDR AS (
  (SELECT ORD_CURCY,
    COMB_ORD_FLG,
    COMB_ORD,
    ORD,
    INV_NBR,
    WHSE,
    ORD_DT,
    ORD_CUST,
    VOL_DISTB,
    ORD_DISTB,
    SHP_DISTB,
    ORD_CANC,
    ORD_SHP,
    PAY_REQ,
    ORD_PAY_STAT,
    ORGNL_ORD,
    ORD_TYPE,
    CUST_SRC,
    ORD_TAX,
    ORD_VALUE,
    ORD_HNDLG_PRICE,
    COALESCE(ORD_TAX/NULLIF(ORD_VALUE-ORD_TAX, 0), 0) AS ORD_TAX_RT
  FROM DWSATM01.DWT42016_ORD_HDR_OMS
  WHERE ORD_CURCY    IN ('360', '050')
  AND DISTB_TYPE NOT IN ('W', 'T')
  AND ORD_DT BETWEEN 20210201 AND 20210231
  )),
  ORDPRMD_AGG AS
  (SELECT ADJ_DET.BINVOD AS "inv_cd",
    OORDDTA.ORD          AS "ord_id",
    TRIM(ADJ_DET.BITSKU) AS "ord_item_cd",
    TRIM(ADJ_DET.BALOTP) AS ADJ_TYPE,
    -SUM(ADJ_DET.BALOAM) AS ADJ_LN_REV,
    -SUM(ADJ_DET.BALOPV) AS ADJ_LN_PV,
    -SUM(ADJ_DET.BALOBV) AS ADJ_LN_BV
  FROM DWSEAI01.ORDPRMD ADJ_DET
  INNER JOIN
    (SELECT INV_NBR, ORD FROM ORD_HDR
    ) OORDDTA
  ON ADJ_DET.BINVOD=OORDDTA.INV_NBR
  GROUP BY ADJ_DET.BINVOD,
    TRIM(ADJ_DET.BITSKU),
    ADJ_DET.BALOTP,
    OORDDTA.ORD
  ),
  /*Coupons missing in the main adjustments table DWSEAI01.ORDPRMD*/
  CCPNAUD_COMPL AS
  (SELECT COUP_RED_ORD,
    -SUM(COUP_RED_VAL) AS CPN_LC,
    -SUM(COUP_RED_BV)  AS CPN_BV,
    -SUM(COUP_RED_PV)  AS CPN_PV
  FROM DWSATM01.DWT43061_COUP_CHN T0
  WHERE COUP_ADT_TYP= 'O'
  AND NOT EXISTS
    (SELECT "ord_id"
    FROM ORDPRMD_AGG T1
    WHERE T1.ADJ_TYPE  ='1'
    AND T0.COUP_RED_ORD=T1."ord_id"
    )
  GROUP BY COUP_RED_ORD
  ),
  OORDDTA AS
  (SELECT CAST(ORD_CURCY AS INTEGER) AS "curcy_id",
    CASE
      WHEN COMB_ORD_FLG='N'
      THEN 'false'
      WHEN COMB_ORD_FLG='Y'
      THEN 'true'
      ELSE NULL
    END                          AS "comb_ord_flag",
    COMB_ORD                     AS "comb_ord_id",
    ORD                          AS "ord_id",
    INV_NBR                      AS "inv_cd",
    ORD_CHNL.GLBL_SALES_CHNL_CD  AS "ord_channel",
    WHSE                         AS "whse_cd",
    CAST(ORD_DT/ 100 AS INTEGER) AS "ord_mo_yr_id",
    TO_DATE(ORD_DT, 'YYYYMMDD')  AS "ord_dt",
    CAST(ORD_CURCY AS INTEGER)   AS "oper_cntry_id",
    ORD_CURCY                    AS "oper_cntry_cd",
    360                          AS "oper_aff_id",
    '360'                        AS "oper_aff_cd",
    CASE
      WHEN ORD_CUST>0
      THEN ORD_CUST
      ELSE
        CASE
          WHEN IMC_TYPES_HIST_ORD.IMC_TYPE_AGG='ABO'
          THEN VOL_DISTB
          ELSE ORD_DISTB
        END
    END AS "account_id",
    360*POWER(10,11)+
    CASE
      WHEN ORD_CUST>0
      THEN ORD_CUST
      ELSE
        CASE
          WHEN IMC_TYPES_HIST_ORD.IMC_TYPE_AGG='ABO'
          THEN VOL_DISTB
          ELSE ORD_DISTB
        END
    END                        AS "global_account_id",
    360*POWER(10,11)+VOL_DISTB AS "vol_global_account_id",
    360*POWER(10,11)+ORD_DISTB AS "ord_global_account_id",
    360*POWER(10,11)+SHP_DISTB AS "shp_global_account_id",
    CASE
      WHEN ORD_CANC ='N'
      THEN 'false'
      WHEN ORD_CANC ='Y'
      THEN 'true'
    END AS "ord_canc_flag",
    CASE
      WHEN ORD_SHP='N'
      THEN 'false'
      WHEN ORD_SHP='Y'
      THEN 'true'
    END AS "ord_shp_flag",
    CASE
      WHEN PAY_REQ='N'
      THEN 'false'
      WHEN PAY_REQ='Y'
      THEN 'true'
    END AS "pay_req_flag",
    CASE
      WHEN ORD_PAY_STAT='P'
      THEN 'true'
      ELSE 'false'
    END       AS "ord_paid_flag",
    ORGNL_ORD AS "org_ord_id",
    ORD_TYPE  AS "ord_type_cd",
    CASE
      WHEN CUST_SRC='S'
      THEN 'true'
      ELSE 'false'
    END AS "sop_flag",
    CASE
      WHEN ORD_CUST>0
      THEN 'true'
      ELSE 'false'
    END                  AS "foa_ord_flag",
    CCPNAUD_COMPL.CPN_LC AS CPN_LC_ORD,
    CCPNAUD_COMPL.CPN_PV AS CPN_PV_ORD,
    CCPNAUD_COMPL.CPN_BV AS CPN_BV_ORD
  FROM ORD_HDR ORD_HDR
  LEFT JOIN DWSATM01.dwt42051_ord_sales_chnl ORD_CHNL
  ON ORD_HDR.ORD_CURCY=ORD_CHNL.CNTRY_CD
  AND ORD_HDR.ORD_TYPE=ORD_CHNL.ORD_TYPE_CD
  LEFT JOIN IMC_TYPES_HIST IMC_TYPES_HIST_ORD
  ON ORD_HDR.ORD_DISTB                   =IMC_TYPES_HIST_ORD.IMC_NO
  AND ORD_HDR.ORD_CURCY                  =IMC_TYPES_HIST_ORD.CNTRY_CD
  AND CAST(ORD_HDR.ORD_DT/100 AS INTEGER)=IMC_TYPES_HIST_ORD.SNAP_MO_YR_KEY_NO
  LEFT JOIN CCPNAUD_COMPL CCPNAUD_COMPL
  ON ORD_HDR.ORD=CCPNAUD_COMPL.COUP_RED_ORD
  ) ,
  OORDLIN AS
  (SELECT ORD                           AS "ord_id",
    ORD_LN                              AS "ord_ln_id",
    CAST(ITEM_DISP AS VARCHAR2(1 CHAR)) AS "ord_ln_disp_cd",
    CASE
      WHEN ITEM_DISP IN ('*', '1', 'E', 'Item Disposition Code:  S/S', 'Item Disposition Code:  S/T', 'S', 'Item Disposition Code:  S/B', 'Item Disposition Code:  S/N', '2')
      THEN 1
      ELSE 0
    END                                   AS NON_TNA_FLAG,
    TRIM(CAST(ITEM AS VARCHAR2(15 CHAR))) AS "ord_item_cd",
    CASE
      WHEN INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(ITEM AS VARCHAR2(15 CHAR))), 4, LENGTH(TRIM(CAST(ITEM AS VARCHAR2(15 CHAR))))                                                 -3), '[B-Z]', 'A'), 'A')>0
      THEN SUBSTR(TRIM(CAST(ITEM AS VARCHAR2(15 CHAR))), 1, INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(ITEM AS VARCHAR2(15 CHAR))), 4, LENGTH(TRIM(CAST(ITEM AS VARCHAR2(15 CHAR))))-3), '[B-Z]', 'A'), 'A')+ 2)
      ELSE TRIM(CAST(ITEM AS VARCHAR2(15 CHAR)))
    END                                         AS "ord_item_base_cd",
    ITEM_QTY                                    AS "ord_item_qty",
    ITEM_PRICE                                  AS "catalog_ln_lc_net",
    ITEM_PV                                     AS "catalog_ln_pv",
    ITEM_BV                                     AS "catalog_ln_bv",
    CAST(0 AS NUMBER(38, 2))                    AS "prm_lc_net",
    CAST(0 AS NUMBER(38, 2))                    AS "prm_pv",
    CAST(0 AS NUMBER(38, 2))                    AS "prm_bv",
    COALESCE(ITEM_TAX/NULLIF(ITEM_PRICE, 0), 0) AS LN_TAX_RT
  FROM DWSATM01.DWT42017_ORD_LN_OMS
  WHERE INTGRT_CNTRY_CD IN ('050', '360')
  UNION ALL
  SELECT ORD_HDR.ORD              AS "ord_id",
    9999                          AS "ord_ln_id",
    CAST('1' AS VARCHAR2(1 CHAR)) AS "ord_ln_disp_cd",
    1                             AS NON_TNA_FLAG,
    'PRM'
    ||TRIM(OPRMITM.ITEM_NO) AS "ord_item_cd",
    'PRM'
    ||TRIM(OPRMITM.ITEM_NO)                                AS "ord_item_base_cd",
    OPRMITM.ITEM_QTY                                       AS "ord_item_qty",
    CAST(0 AS NUMBER(38, 2))                               AS "catalog_ln_lc_net",
    CAST(0 AS NUMBER(38, 2))                               AS "catalog_ln_pv",
    CAST(0 AS NUMBER(38, 2))                               AS "catalog_ln_bv",
    -ROUND(OPRMITM.ITEM_PRICE_DP/(1+ORD_HDR.ORD_TAX_RT),2) AS "prm_lc_net",
    -OPRMITM.ITEM_PRICE_PV                                 AS "prm_pv",
    -OPRMITM.ITEM_PRICE_BV                                 AS "prm_bv",
    0                                                      AS LN_TAX_RT
  FROM DWSATM01.DWT43062_DSCNT_CHN OPRMITM
  JOIN ORD_HDR ORD_HDR
  ON OPRMITM.INV_NO     =ORD_HDR.INV_NBR
  WHERE OPRMITM.REC_TYP ='O'
  AND OPRMITM.ITEM_TYP  = 'D'
    /*For dummy items, keep only those discounts which are not presented in the main table with adjustments - ORDPRMD*/
  AND NOT EXISTS
    (SELECT "ord_id"
    FROM ORDPRMD_AGG T1
    WHERE T1.ADJ_TYPE ='3'
    AND OPRMITM.INV_NO=T1."inv_cd"
    )
  )
-----Main
SELECT OORDDTA."curcy_id",
  OORDDTA."comb_ord_flag",
  OORDDTA."comb_ord_id",
  OORDDTA."ord_id",
  OORDDTA."inv_cd",
  OORDDTA."ord_channel",
  OORDDTA."whse_cd",
  OORDDTA."ord_mo_yr_id",
  OORDDTA."ord_dt",
  OORDDTA."oper_cntry_id",
  OORDDTA."oper_cntry_cd",
  OORDDTA."oper_aff_id",
  OORDDTA."oper_aff_cd",
  OORDDTA."account_id",
  OORDDTA."global_account_id",
  OORDDTA."vol_global_account_id",
  OORDDTA."ord_global_account_id",
  OORDDTA."shp_global_account_id",
  OORDDTA."ord_canc_flag",
  OORDDTA."ord_shp_flag",
  OORDDTA."pay_req_flag",
  OORDDTA."ord_paid_flag",
  OORDDTA."org_ord_id",
  OORDDTA."ord_type_cd",
  OORDDTA."sop_flag",
  OORDDTA."foa_ord_flag",
  OORDLIN."ord_ln_disp_cd",
  OORDLIN."ord_ln_id",
  OORDLIN."ord_item_cd",
  OORDLIN."ord_item_base_cd",
  OORDLIN."ord_item_qty",
  OORDLIN."catalog_ln_lc_net",
  OORDLIN."catalog_ln_pv",
  OORDLIN."catalog_ln_bv",
  ROUND(COALESCE(PRM_LN_MAIN.ADJ_LN_REV, 0)/(1+OORDLIN.LN_TAX_RT),2)+OORDLIN."prm_lc_net"                                                                                                                                                                                                                                                                                                                                                             AS "prm_lc_net",
  COALESCE(PRM_LN_MAIN.ADJ_LN_PV, 0)       +OORDLIN."prm_pv"                                                                                                                                                                                                                                                                                                                                                                                          AS "prm_pv",
  COALESCE(PRM_LN_MAIN.ADJ_LN_BV, 0)       +OORDLIN."prm_bv"                                                                                                                                                                                                                                                                                                                                                                                          AS "prm_bv",
  ROUND(COALESCE(CPN_LN_MAIN.ADJ_LN_REV, 0)/(1+OORDLIN.LN_TAX_RT),2) + ROUND(COALESCE(OORDDTA.CPN_LC_ORD,0)/(1+OORDLIN.LN_TAX_RT)*(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_lc_net"/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_lc_net") OVER(PARTITION BY OORDLIN."ord_id", OORDDTA."oper_cntry_id"), 0)),2)                                                                                                                                 AS "cpn_lc_net",
  COALESCE(CPN_LN_MAIN.ADJ_LN_PV, 0)       +ROUND(COALESCE(OORDDTA.CPN_PV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_pv"/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_pv") OVER(PARTITION BY OORDLIN."ord_id", OORDDTA."oper_cntry_id"), 0)),2)                                                                                                                                                                                          AS "cpn_pv",
  COALESCE(CPN_LN_MAIN.ADJ_LN_BV, 0)       +ROUND(COALESCE(OORDDTA.CPN_BV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_bv"/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_bv") OVER(PARTITION BY OORDLIN."ord_id", OORDDTA."oper_cntry_id"), 0)),2)                                                                                                                                                                                          AS "cpn_bv",
  OORDLIN."catalog_ln_lc_net"              +OORDLIN."prm_lc_net"+ROUND(COALESCE(PRM_LN_MAIN.ADJ_LN_REV, 0)/(1+OORDLIN.LN_TAX_RT),2)+ROUND(COALESCE(CPN_LN_MAIN.ADJ_LN_REV, 0)/(1+OORDLIN.LN_TAX_RT),2)+ROUND(COALESCE(OORDDTA.CPN_LC_ORD,0)/(1+OORDLIN.LN_TAX_RT)*(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_lc_net"/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_lc_net") OVER(PARTITION BY OORDLIN."ord_id", OORDDTA."oper_cntry_id"), 0)),2) AS "adj_ln_lc_net",
  OORDLIN."catalog_ln_pv"                  +COALESCE(PRM_LN_MAIN.ADJ_LN_PV, 0)+OORDLIN."prm_pv"+COALESCE(CPN_LN_MAIN.ADJ_LN_PV, 0)+ROUND(COALESCE(OORDDTA.CPN_PV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_pv"/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_pv") OVER(PARTITION BY OORDLIN."ord_id", OORDDTA."oper_cntry_id"), 0)),2)                                                                                                   AS "adj_ln_pv",
  OORDLIN."catalog_ln_bv"                  +COALESCE(PRM_LN_MAIN.ADJ_LN_BV, 0)+OORDLIN."prm_bv"+COALESCE(CPN_LN_MAIN.ADJ_LN_BV, 0)+ROUND(COALESCE(OORDDTA.CPN_BV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_bv"/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN."catalog_ln_bv") OVER(PARTITION BY OORDLIN."ord_id", OORDDTA."oper_cntry_id"), 0)),2)                                                                                                   AS "adj_ln_bv"
FROM OORDDTA OORDDTA
LEFT JOIN OORDLIN OORDLIN
ON OORDDTA."ord_id"=OORDLIN."ord_id"
LEFT JOIN
  (SELECT * FROM ORDPRMD_AGG WHERE ADJ_TYPE='1'
  ) CPN_LN_MAIN
ON OORDDTA."inv_cd"      =CPN_LN_MAIN."inv_cd"
AND OORDLIN."ord_item_cd"=CPN_LN_MAIN."ord_item_cd"
LEFT JOIN
  (SELECT * FROM ORDPRMD_AGG WHERE ADJ_TYPE='3'
  ) PRM_LN_MAIN
ON OORDDTA."inv_cd"      =PRM_LN_MAIN."inv_cd"
AND OORDLIN."ord_item_cd"=PRM_LN_MAIN."ord_item_cd"
  );
COMMIT;
