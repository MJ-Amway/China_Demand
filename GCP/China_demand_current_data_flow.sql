INSERT INTO `amw-dna-coe-working-dev`.china_demand.demand_sales_base
SELECT *
FROM
  (WITH IMC_TYPES_HIST AS (SELECT account_id,
  snap_mo_yr_key_no,
  cntry_cd,
  CASE
    WHEN TRIM(bus_natr_cd) IN ('C', '3', 'V')
    THEN 'RC'
    ELSE 'ABO'
  END AS imc_type_agg FROM `amw-dna-coe-curated`.account_master.account_master_snapshot
WHERE snap_mo_yr_key_no BETWEEN 202101 AND 202102 AND cntry_id IN (360, 50)),
IMC_TYPES_CURR AS (SELECT account_id,
  cntry_cd,
  CASE
    WHEN TRIM(bus_natr_cd) IN ('C', '3', 'V')
    THEN 'RC'
    ELSE 'ABO'
  END AS imc_type_agg FROM `amw-dna-coe-curated`.account_master.account_master_flatten
WHERE cntry_id IN (360, 50)),
ORD_HDR AS ((SELECT ORD_CURCY, COMB_ORD_FLG, COMB_ORD, ORD, INV_NBR, WHSE, ORD_DT, ORD_CUST,COALESCE(TRIM(CAST(ORD_SRC AS STRING)), '*') AS ORD_SRC, VOL_DISTB, ORD_DISTB, SHP_DISTB, ORD_CANC, ORD_SHP, PAY_REQ, ORD_PAY_STAT, ORGNL_ORD, ORD_TYPE, CUST_SRC, ORD_TAX, ORD_VALUE, ORD_HNDLG_PRICE, COALESCE(ORD_TAX/NULLIF(ORD_VALUE-ORD_TAX, 0), 0) AS ORD_TAX_RT FROM `amw-dna-ingestion-prd`.gdw_atomic.dwt42016_ord_hdr_oms
WHERE ORD_CURCY IN ('360', '050')
    AND DISTB_TYPE NOT IN ('W', 'T')
    AND ORD_DT BETWEEN 20210101 AND 20210231)),
  ORDPRMD_AGG AS
  (SELECT ADJ_DET.BINVOD AS inv_cd,
    OORDDTA.ORD          AS ord_id,
    TRIM(ADJ_DET.BITSKU) AS ord_item_cd,
    TRIM(ADJ_DET.BALOTP) AS ADJ_TYPE,
    -SUM(ADJ_DET.BALOAM) AS ADJ_LN_REV,
    -SUM(ADJ_DET.BALOPV) AS ADJ_LN_PV,
    -SUM(ADJ_DET.BALOBV) AS ADJ_LN_BV
  FROM `amw-dna-coe-working-dev`.ingested.ordprmd ADJ_DET
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
  FROM `amw-dna-coe-working-dev`.ingested.dwt43061_coup_chn T0
  WHERE COUP_ADT_TYP= 'O'
  AND NOT EXISTS
    (SELECT ord_id
    FROM ORDPRMD_AGG T1
    WHERE T1.ADJ_TYPE  ='1'
    AND T0.COUP_RED_ORD=T1.ord_id
    )
  GROUP BY COUP_RED_ORD
  ),
 OORDDTA AS (
SELECT 
CAST(CASE WHEN ORD_CURCY='360' THEN 'CNY' WHEN ORD_CURCY='050' THEN 'HKD' ELSE 'NA' END AS STRING) AS curcy_id,
CASE
        WHEN COMB_ORD_FLG='N'
        THEN false
        WHEN COMB_ORD_FLG='Y'
        THEN true
        ELSE NULL
      END AS comb_ord_flag,
COMB_ORD AS comb_ord_id,
CAST(ORD AS INT64) AS ord_id,
INV_NBR AS inv_cd,
ORD_CHNL.GLBL_SALES_CHNL_CD as ord_channel,
WHSE AS whse_cd,
CAST(ORD_DT/ 100 AS INT64) AS ord_mo_yr_id,
PARSE_DATETIME('%Y-%m-%d', SUBSTR(CAST(ORD_DT AS STRING), 1, 4)||'-'||SUBSTR(CAST(ORD_DT AS STRING), 5, 2)||'-'||SUBSTR(CAST(ORD_DT AS STRING), 7, 2)) as ord_dt,
CAST(ORD_CURCY AS INT64) AS oper_cntry_id,
ORD_CURCY AS oper_cntry_cd,
360  AS oper_aff_id,
'360' AS oper_aff_cd,
CASE WHEN ORD_CUST>0 THEN ORD_CUST
ELSE CASE WHEN COALESCE(IMC_TYPES_HIST_ORD.IMC_TYPE_AGG, IMC_TYPES_CURR_ORD.IMC_TYPE_AGG)='ABO'
THEN VOL_DISTB ELSE ORD_DISTB END
END as account_id,
360*POWER(10,11)+CASE WHEN ORD_CUST>0 THEN ORD_CUST
ELSE CASE WHEN COALESCE(IMC_TYPES_HIST_ORD.IMC_TYPE_AGG, IMC_TYPES_CURR_ORD.IMC_TYPE_AGG)='ABO'
THEN VOL_DISTB ELSE ORD_DISTB END
END as global_account_id,
360*POWER(10,11)+VOL_DISTB AS vol_global_account_id,
360*POWER(10,11)+ORD_DISTB AS ord_global_account_id,
360*POWER(10,11)+SHP_DISTB AS shp_global_account_id,
CASE WHEN ORD_CANC ='N' THEN false WHEN ORD_CANC ='Y' THEN true END AS ord_canc_flag,
CASE WHEN ORD_SHP='N' THEN false WHEN ORD_SHP='Y' THEN true END AS ord_shp_flag,
CASE WHEN PAY_REQ='N' THEN false WHEN PAY_REQ='Y' THEN true END AS pay_req_flag,
CASE WHEN ORD_PAY_STAT='P' THEN true ELSE false END AS ord_paid_flag,
ORGNL_ORD AS org_ord_id,
ORD_TYPE AS ord_type_cd,
CASE WHEN CUST_SRC='S' THEN true ELSE false END as sop_flag,
CASE WHEN ORD_CUST>0 THEN true ELSE false END as foa_ord_flag,
/*ORD_TAX_RT,*/
ORD_HNDLG_PRICE/(1+ORD_TAX_RT) AS DELIVERY_FEE_HK,
CCPNAUD_COMPL.CPN_LC AS CPN_LC_ORD,
CCPNAUD_COMPL.CPN_PV AS CPN_PV_ORD,
CCPNAUD_COMPL.CPN_BV AS CPN_BV_ORD
FROM ORD_HDR ORD_HDR
    LEFT JOIN
    `amw-dna-ingestion-prd`.gdw_atomic.dwt42051_ord_sales_chnl ORD_CHNL
    ON ORD_HDR.ORD_CURCY=ORD_CHNL.CNTRY_CD
    AND ORD_HDR.ORD_TYPE=ORD_CHNL.ORD_TYPE_CD
    AND ORD_HDR.ORD_SRC=ORD_CHNL.ORD_SRC_CD 
    LEFT JOIN IMC_TYPES_HIST IMC_TYPES_HIST_ORD
    ON ORD_HDR.ORD_DISTB=IMC_TYPES_HIST_ORD.account_id
    AND ORD_HDR.ORD_CURCY=IMC_TYPES_HIST_ORD.CNTRY_CD
    AND CAST(ORD_HDR.ORD_DT/100 AS INT64)=IMC_TYPES_HIST_ORD.SNAP_MO_YR_KEY_NO
    LEFT JOIN IMC_TYPES_CURR IMC_TYPES_CURR_ORD
    ON ORD_HDR.ORD_DISTB=IMC_TYPES_CURR_ORD.account_id
    AND ORD_HDR.ORD_CURCY=IMC_TYPES_CURR_ORD.CNTRY_CD
    LEFT JOIN CCPNAUD_COMPL CCPNAUD_COMPL
    ON ORD_HDR.ORD=CCPNAUD_COMPL.COUP_RED_ORD
),
  OORDLIN AS
  (SELECT CAST(ORD AS INT64) AS ord_id,
ORD_LN AS ord_ln_id,
CAST(ITEM_DISP AS string) AS ord_ln_disp_cd,
CASE WHEN ITEM_DISP IN ('*', '1', 'E', 'Item Disposition Code:  S/S', 'Item Disposition Code:  S/T', 'S', 'Item Disposition Code:  S/B', 'Item Disposition Code:  S/N', '2') THEN 1 else 0 END AS NON_TNA_FLAG,
TRIM(CAST(ITEM AS string)) as ord_item_cd,
CASE
  WHEN INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(ITEM AS string)), 4, GREATEST(0,LENGTH(TRIM(CAST(ITEM AS string)))                      -3)), '[B-Z]', 'A'), 'A')>0
  THEN SUBSTR(TRIM(CAST(ITEM AS string)), 1, INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(ITEM AS string)), 4, GREATEST(0, LENGTH(TRIM(CAST(ITEM AS string)))-3)), '[B-Z]', 'A'), 'A')+ 2)
  ELSE TRIM(CAST(ITEM AS string))
END AS ord_item_base_cd,
ITEM_QTY as ord_item_qty,
ITEM_PRICE as catalog_ln_lc_net,
ITEM_PV AS catalog_ln_pv,
ITEM_BV AS catalog_ln_bv,
CAST(0 AS numeric) AS prm_lc_net,
CAST(0 AS numeric) AS prm_pv,
CAST(0 AS numeric) AS prm_bv,
COALESCE(ITEM_TAX/NULLIF(ITEM_PRICE, 0), 0) AS LN_TAX_RT
FROM `amw-dna-ingestion-prd`.gdw_atomic.dwt42017_ord_ln_oms WHERE INTGRT_CNTRY_CD IN ('050', '360')
  UNION ALL
  SELECT CAST(ORD_HDR.ORD AS INT64)              AS ord_id,
    9999                          AS ord_ln_id,
    CAST('1' AS string) AS ord_ln_disp_cd,
    1                             AS NON_TNA_FLAG,
    'PRM'||OPRMITM.ITEM_NO AS ord_item_cd,
    'PRM'||OPRMITM.ITEM_NO                                AS ord_item_base_cd,
    OPRMITM.ITEM_QTY                                       AS ord_item_qty,
    CAST(0 AS numeric)                               AS catalog_ln_lc_net,
    CAST(0 AS numeric)                               AS catalog_ln_pv,
    CAST(0 AS numeric)                               AS catalog_ln_bv,
    -ROUND(OPRMITM.ITEM_PRICE_DP/(1+ORD_HDR.ORD_TAX_RT),2) AS prm_lc_net,
    -OPRMITM.ITEM_PRICE_PV                                 AS prm_pv,
    -OPRMITM.ITEM_PRICE_BV                                 AS prm_bv,
    0                                                      AS LN_TAX_RT
FROM `amw-dna-coe-working-dev`.ingested.dwt43062_dscnt_chn OPRMITM
  JOIN ORD_HDR ORD_HDR
  ON OPRMITM.INV_NO     =ORD_HDR.INV_NBR
  WHERE OPRMITM.REC_TYP ='O'
  AND OPRMITM.ITEM_TYP  = 'D'
    /*For dummy items, keep only those discounts which are not presented in the main table with adjustments - ORDPRMD*/
  AND NOT EXISTS
    (SELECT ord_id
    FROM ORDPRMD_AGG T1
    WHERE T1.ADJ_TYPE ='3'
    AND OPRMITM.INV_NO=T1.inv_cd
    )
  )
-----Main
SELECT OORDDTA.curcy_id,
  OORDDTA.comb_ord_flag,
CAST(OORDDTA.comb_ord_id AS INT64) AS comb_ord_id,
  OORDDTA.ord_id,
  OORDDTA.inv_cd,
  OORDDTA.ord_channel,
  OORDDTA.whse_cd,
  OORDDTA.ord_mo_yr_id,
  OORDDTA.ord_dt,
  OORDDTA.oper_cntry_id,
  OORDDTA.oper_cntry_cd,
  OORDDTA.oper_aff_id,
  OORDDTA.oper_aff_cd,
CAST(OORDDTA.account_id as INT64) AS account_id,
CAST(OORDDTA.global_account_id AS INT64) AS global_account_id,
CAST(OORDDTA.vol_global_account_id AS INT64) AS vol_global_account_id,
CAST(OORDDTA.ord_global_account_id AS INT64) AS ord_global_account_id,
CAST(OORDDTA.shp_global_account_id AS INT64) AS shp_global_account_id,
  OORDDTA.ord_canc_flag,
  OORDDTA.ord_shp_flag,
  OORDDTA.pay_req_flag,
  OORDDTA.ord_paid_flag,
CAST(OORDDTA.org_ord_id AS INT64) AS org_ord_id,
  OORDDTA.ord_type_cd,
  OORDDTA.sop_flag,
  OORDDTA.foa_ord_flag,
  OORDLIN.ord_ln_disp_cd,
CAST(OORDLIN.ord_ln_id AS INT64) AS ord_ln_id,
  OORDLIN.ord_item_cd,
  OORDLIN.ord_item_base_cd,
CAST(OORDLIN.ord_item_qty AS INT64) AS ord_item_qty,
  OORDLIN.catalog_ln_lc_net,
  OORDLIN.catalog_ln_pv,
  OORDLIN.catalog_ln_bv,
  ROUND(COALESCE(PRM_LN_MAIN.ADJ_LN_REV, 0)/(1+OORDLIN.LN_TAX_RT),2)+OORDLIN.prm_lc_net                                                                                                                                                                                                                                                                                                                                                             AS prm_lc_net,
  COALESCE(PRM_LN_MAIN.ADJ_LN_PV, 0)       +OORDLIN.prm_pv                                                                                                                                                                                                                                                                                                                                                                                          AS prm_pv,
  COALESCE(PRM_LN_MAIN.ADJ_LN_BV, 0)       +OORDLIN.prm_bv                                                                                                                                                                                                                                                                                                                                                                                          AS prm_bv,
  ROUND(COALESCE(CPN_LN_MAIN.ADJ_LN_REV, 0)/(1+OORDLIN.LN_TAX_RT),2) + ROUND(COALESCE(OORDDTA.CPN_LC_ORD,0)/(1+OORDLIN.LN_TAX_RT)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_lc_net/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_lc_net) OVER(PARTITION BY OORDLIN.ord_id, OORDDTA.oper_cntry_id), 0)),2)                                                                                                                                 AS cpn_lc_net,
  COALESCE(CPN_LN_MAIN.ADJ_LN_PV, 0)       +ROUND(COALESCE(OORDDTA.CPN_PV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_pv/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_pv) OVER(PARTITION BY OORDLIN.ord_id, OORDDTA.oper_cntry_id), 0)),2)                                                                                                                                                                                          AS cpn_pv,
  COALESCE(CPN_LN_MAIN.ADJ_LN_BV, 0)       +ROUND(COALESCE(OORDDTA.CPN_BV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_bv/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_bv) OVER(PARTITION BY OORDLIN.ord_id, OORDDTA.oper_cntry_id), 0)),2)                                                                                                                                                                                          AS cpn_bv,
  OORDLIN.catalog_ln_lc_net              +OORDLIN.prm_lc_net+ROUND(COALESCE(PRM_LN_MAIN.ADJ_LN_REV, 0)/(1+OORDLIN.LN_TAX_RT),2)+ROUND(COALESCE(CPN_LN_MAIN.ADJ_LN_REV, 0)/(1+OORDLIN.LN_TAX_RT),2)+ROUND(COALESCE(OORDDTA.CPN_LC_ORD,0)/(1+OORDLIN.LN_TAX_RT)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_lc_net/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_lc_net) OVER(PARTITION BY OORDLIN.ord_id, OORDDTA.oper_cntry_id), 0)),2) AS adj_ln_lc_net,
  OORDLIN.catalog_ln_pv                  +COALESCE(PRM_LN_MAIN.ADJ_LN_PV, 0)+OORDLIN.prm_pv+COALESCE(CPN_LN_MAIN.ADJ_LN_PV, 0)+ROUND(COALESCE(OORDDTA.CPN_PV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_pv/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_pv) OVER(PARTITION BY OORDLIN.ord_id, OORDDTA.oper_cntry_id), 0)),2)                                                                                                   AS adj_ln_pv,
  OORDLIN.catalog_ln_bv                  +COALESCE(PRM_LN_MAIN.ADJ_LN_BV, 0)+OORDLIN.prm_bv+COALESCE(CPN_LN_MAIN.ADJ_LN_BV, 0)+ROUND(COALESCE(OORDDTA.CPN_BV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_bv/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_bv) OVER(PARTITION BY OORDLIN.ord_id, OORDDTA.oper_cntry_id), 0)),2)                                                                                                   AS adj_ln_bv,
ord_hdr_5gpos.delivery_fee_lc_net
FROM OORDDTA OORDDTA
LEFT JOIN OORDLIN OORDLIN
ON OORDDTA.ord_id=OORDLIN.ord_id
LEFT JOIN
  (SELECT * FROM ORDPRMD_AGG WHERE ADJ_TYPE='1'
  ) CPN_LN_MAIN
ON OORDDTA.inv_cd      =CPN_LN_MAIN.inv_cd
AND OORDLIN.ord_item_cd=CPN_LN_MAIN.ord_item_cd
LEFT JOIN
  (SELECT * FROM ORDPRMD_AGG WHERE ADJ_TYPE='3'
  ) PRM_LN_MAIN
ON OORDDTA.inv_cd      =PRM_LN_MAIN.inv_cd
AND OORDLIN.ord_item_cd=PRM_LN_MAIN.ord_item_cd
LEFT JOIN (SELECT SUBSTR(order_number,-11) AS inv_cd, round(avg(delivery_charge)/1.13, 2) as delivery_fee_lc_net from `amw-dna-ingestion-prd`.`5gpos_order`.mstb_order_header
where EXTRACT(YEAR FROM SALE_DATE)*100+EXTRACT(MONTH FROM SALE_DATE) BETWEEN 202101 AND 202102
AND order_type IN('OO','MM')
AND region_code = '360' group by SUBSTR(order_number,-11)) ord_hdr_5gpos
ON OORDDTA.inv_cd=ord_hdr_5gpos.inv_cd)
