CREATE OR REPLACE TABLE `amw-dna-coe-working-dev`.china_demand.demand_sales_base as select * from 
(WITH 
IMC_TYPES_HIST AS (SELECT account_id,
  snap_mo_yr_key_no,
  cntry_cd,
  CASE
    WHEN TRIM(bus_natr_cd) IN ('C', '3', 'V')
    THEN 'RC'
    ELSE 'ABO'
  END AS imc_type_agg FROM `amw-dna-coe-curated`.account_master.account_master_snapshot
WHERE snap_mo_yr_key_no BETWEEN 202001 AND 202001 AND cntry_id IN (360, 50)),
ORD_HDR AS ((SELECT ORD_CURCY, COMB_ORD_FLG, COMB_ORD, ORD, INV_NBR, WHSE, ORD_DT, ORD_CUST, VOL_DISTB, ORD_DISTB, SHP_DISTB, ORD_CANC, ORD_SHP, PAY_REQ, ORD_PAY_STAT, ORGNL_ORD, ORD_TYPE, CUST_SRC, ORD_TAX, ORD_VALUE, ORD_HNDLG_PRICE, COALESCE(ORD_TAX/NULLIF(ORD_VALUE-ORD_TAX, 0), 0) AS ORD_TAX_RT FROM `amw-dna-ingestion-prd`.gdw_atomic.dwt42016_ord_hdr_oms
WHERE ORD_CURCY IN ('360', '050')
    AND DISTB_TYPE NOT IN ('W', 'T')
    AND ORD_DT BETWEEN 20200101 AND 20200131)),
CCPNAUD AS (SELECT COUP_RED_ORD,
        -SUM(COUP_RED_VAL) AS CPN_LC,
        -SUM(COUP_RED_BV)  AS CPN_BV,
        -SUM(COUP_RED_PV)  AS CPN_PV
      FROM `amw-dna-coe-working-dev`.ingested.dwt43061_coup_chn
      WHERE COUP_ADT_TYP= 'O'
      GROUP BY COUP_RED_ORD
      ),
OORDDTA AS (
SELECT 
CAST(ORD_CURCY AS INT64) AS curcy_id,
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
ELSE CASE WHEN IMC_TYPES_HIST_ORD.imc_type_agg='ABO'
THEN VOL_DISTB ELSE ORD_DISTB END
END as account_id,
360*POWER(10,11)+CASE WHEN ORD_CUST>0 THEN ORD_CUST
ELSE CASE WHEN IMC_TYPES_HIST_ORD.IMC_TYPE_AGG='ABO'
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
CCPNAUD.CPN_LC AS CPN_LC_ORD,
CCPNAUD.CPN_PV AS CPN_PV_ORD,
CCPNAUD.CPN_BV AS CPN_BV_ORD
FROM ORD_HDR ORD_HDR
    LEFT JOIN
    `amw-dna-ingestion-prd`.gdw_atomic.dwt42051_ord_sales_chnl ORD_CHNL
    ON ORD_HDR.ORD_CURCY=ORD_CHNL.CNTRY_CD
    AND ORD_HDR.ORD_TYPE=ORD_CHNL.ORD_TYPE_CD
    LEFT JOIN IMC_TYPES_HIST IMC_TYPES_HIST_ORD
    ON ORD_HDR.ORD_DISTB=IMC_TYPES_HIST_ORD.account_id
    AND ORD_HDR.ORD_CURCY=IMC_TYPES_HIST_ORD.CNTRY_CD
    AND CAST(ORD_HDR.ORD_DT/100 AS INT64)=IMC_TYPES_HIST_ORD.SNAP_MO_YR_KEY_NO
    LEFT JOIN CCPNAUD CCPNAUD
    ON ORD_HDR.ORD=CCPNAUD.COUP_RED_ORD
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
SELECT ORD_HDR.ORD AS ord_id,
9999 AS ord_ln_id,
CAST('1' AS string) AS ord_ln_disp_cd,
1 as NON_TNA_FLAG,
'PRM'||OPRMITM.ITEM_NO AS ord_item_cd,
'PRM'||OPRMITM.ITEM_NO AS ord_item_base_cd,
OPRMITM.ITEM_QTY AS ord_item_qty,
CAST(0 AS numeric) AS catalog_ln_lc_net,
CAST(0 AS numeric) AS catalog_ln_pv,
CAST(0 AS numeric) AS catalog_ln_bv,
-ROUND(OPRMITM.ITEM_PRICE_DP/(1+ORD_HDR.ORD_TAX_RT),2) AS prm_lc_net,
-OPRMITM.ITEM_PRICE_PV as prm_pv,
-OPRMITM.ITEM_PRICE_BV as prm_bv,
0 AS LN_TAX_RT
FROM `amw-dna-coe-working-dev`.ingested.dwt43062_dscnt_chn OPRMITM
JOIN ORD_HDR ORD_HDR
ON OPRMITM.INV_NO=ORD_HDR.INV_NBR
      WHERE OPRMITM.REC_TYP ='O'
      AND OPRMITM.ITEM_TYP  = 'D'
)
-----Main
SELECT  OORDDTA.curcy_id,
OORDDTA.comb_ord_flag,
OORDDTA.comb_ord_id,
ord_id,
OORDDTA.inv_cd,
OORDDTA.ord_channel,
OORDDTA.whse_cd,
OORDDTA.ord_mo_yr_id,
OORDDTA.ORD_DT,
OORDDTA.oper_cntry_id,
OORDDTA.oper_cntry_cd,
OORDDTA.oper_aff_id,
OORDDTA.oper_aff_cd,
OORDDTA.account_id,
OORDDTA.global_account_id,
OORDDTA.vol_global_account_id,
OORDDTA.ord_global_account_id,
OORDDTA.shp_global_account_id,
OORDDTA.ord_canc_flag,
OORDDTA.ord_shp_flag,
OORDDTA.pay_req_flag,
OORDDTA.ord_paid_flag,
OORDDTA.org_ord_id,
OORDDTA.ord_type_cd,
OORDDTA.sop_flag,
OORDDTA.foa_ord_flag,
OORDLIN.ord_ln_disp_cd,
OORDLIN.ord_ln_id,
OORDLIN.ord_item_cd,
OORDLIN.ord_item_base_cd,
OORDLIN.ord_item_qty,
OORDLIN.catalog_ln_lc_net,
OORDLIN.catalog_ln_pv,
OORDLIN.catalog_ln_bv,
OORDLIN.prm_lc_net,
OORDLIN.prm_pv,
OORDLIN.prm_bv,
ROUND(COALESCE(OORDDTA.CPN_LC_ORD,0)/(1+OORDLIN.LN_TAX_RT)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_lc_net/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_lc_net) OVER(PARTITION BY ord_id, OORDDTA.oper_cntry_id), 0)),2)  AS cpn_lc_net,
ROUND(COALESCE(OORDDTA.CPN_PV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_pv/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_pv) OVER(PARTITION BY ord_id, OORDDTA.oper_cntry_id), 0)),2)  AS cpn_pv,
ROUND(COALESCE(OORDDTA.CPN_BV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_bv/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_bv) OVER(PARTITION BY ord_id, OORDDTA.oper_cntry_id), 0)),2)  AS cpn_bv,
OORDLIN.catalog_ln_lc_net+OORDLIN.prm_lc_net+ROUND(COALESCE(OORDDTA.CPN_LC_ORD,0)/(1+OORDLIN.LN_TAX_RT)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_lc_net/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_lc_net) OVER(PARTITION BY ord_id, OORDDTA.oper_cntry_id), 0)),2)  AS adj_ln_lc_net,
OORDLIN.catalog_ln_pv+OORDLIN.prm_pv+ROUND(COALESCE(OORDDTA.CPN_PV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_pv/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_pv) OVER(PARTITION BY ord_id, OORDDTA.oper_cntry_id), 0)),2)  AS adj_ln_pv,
OORDLIN.catalog_ln_bv+OORDLIN.prm_bv+ROUND(COALESCE(OORDDTA.CPN_BV_ORD,0)*(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_bv/NULLIF(SUM(OORDLIN.NON_TNA_FLAG*OORDLIN.catalog_ln_bv) OVER(PARTITION BY ord_id, OORDDTA.oper_cntry_id), 0)),2)  AS adj_ln_bv/*,
ord_hdr_5gpos.delivery_fee_lc_net*/
FROM OORDDTA OORDDTA
LEFT JOIN OORDLIN OORDLIN
USING(ord_id)
LEFT JOIN (SELECT SUBSTR(order_number,-11) AS inv_cd, round(avg(delivery_charge)/1.13, 2) as delivery_fee_lc_net from `amw-dna-ingestion-prd`.`5gpos_order`.mstb_order_header
where EXTRACT(YEAR FROM SALE_DATE)*100+EXTRACT(MONTH FROM SALE_DATE) BETWEEN 202001 AND 202012
AND order_type IN('OO','MM')
AND region_code = '360' group by SUBSTR(order_number,-11)) ord_hdr_5gpos
USING(inv_cd))
