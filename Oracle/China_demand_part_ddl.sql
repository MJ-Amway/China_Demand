CREATE TABLE DWSEAI01.CHINA_DEMAND_PART
("curcy_id"	VARCHAR2(3 CHAR),
"comb_ord_flag"	VARCHAR2(5 BYTE),
"comb_ord_id"	NUMBER,
"ord_id"	NUMBER,
"inv_cd"	VARCHAR2(11 CHAR),
"ord_channel"	VARCHAR2(100 BYTE),
"whse_cd" VARCHAR2(8 CHAR),
"ord_mo_yr_id"	NUMBER(38,0),
"ord_dt"	DATE,
"oper_cntry_id"	NUMBER(38,0),
"oper_cntry_cd"	VARCHAR2(3 CHAR),
"oper_aff_id"	NUMBER,
"oper_aff_cd"	CHAR(3 BYTE),
"account_id"	NUMBER(11,0),
"global_account_id"	NUMBER,
"vol_global_account_id"	NUMBER,
"ord_global_account_id"	NUMBER,
"shp_global_account_id"	NUMBER,
"ord_canc_flag"	VARCHAR2(5 BYTE),
"ord_shp_flag"	VARCHAR2(5 BYTE),
"pay_req_flag"	VARCHAR2(5 BYTE),
"ord_paid_flag"	VARCHAR2(5 BYTE),
"org_ord_id"	NUMBER,
"ord_type_cd"	VARCHAR2(3 CHAR),
"sop_flag"	VARCHAR2(5 BYTE),
"foa_ord_flag"	VARCHAR2(5 BYTE),
"ord_ln_disp_cd"	VARCHAR2(1 CHAR),
"ord_ln_id"	NUMBER,
"ord_item_cd"	VARCHAR2(60 BYTE),
"ord_item_base_cd"	VARCHAR2(60 BYTE),
"ord_item_qty"	NUMBER,
"catalog_ln_lc_net"	NUMBER,
"catalog_ln_pv"	NUMBER,
"catalog_ln_bv"	NUMBER,
"prm_lc_net"	NUMBER,
"prm_pv"	NUMBER,
"prm_bv"	NUMBER,
"cpn_lc_net"	NUMBER,
"cpn_pv"	NUMBER,
"cpn_bv"	NUMBER,
"adj_ln_lc_net"	NUMBER,
"adj_ln_pv"	NUMBER,
"adj_ln_bv"	NUMBER)
NOLOGGING 
TABLESPACE DWRGDW01_ORDERS 
PCTFREE 10 
INITRANS 1 
STORAGE 
( 
  BUFFER_POOL DEFAULT 
) 
NOCOMPRESS 
PARALLEL 8 
PARTITION BY LIST ("ord_mo_yr_id") 
(
  PARTITION CHINA_DEMAND_201201 VALUES (201201) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201202 VALUES (201202) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201203 VALUES (201203) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201204 VALUES (201204) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201205 VALUES (201205) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201206 VALUES (201206) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201207 VALUES (201207) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201208 VALUES (201208) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201209 VALUES (201209) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201210 VALUES (201210) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201211 VALUES (201211) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201212 VALUES (201212) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201301 VALUES (201301) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201302 VALUES (201302) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201303 VALUES (201303) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201304 VALUES (201304) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201305 VALUES (201305) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201306 VALUES (201306) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201307 VALUES (201307) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201308 VALUES (201308) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201309 VALUES (201309) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201310 VALUES (201310) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201311 VALUES (201311) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201312 VALUES (201312) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201401 VALUES (201401) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201402 VALUES (201402) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201403 VALUES (201403) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201404 VALUES (201404) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201405 VALUES (201405) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201406 VALUES (201406) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201407 VALUES (201407) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201408 VALUES (201408) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201409 VALUES (201409) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201410 VALUES (201410) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201411 VALUES (201411) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201412 VALUES (201412) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201501 VALUES (201501) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201502 VALUES (201502) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201503 VALUES (201503) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201504 VALUES (201504) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201505 VALUES (201505) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201506 VALUES (201506) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201507 VALUES (201507) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201508 VALUES (201508) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201509 VALUES (201509) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201510 VALUES (201510) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201511 VALUES (201511) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201512 VALUES (201512) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201601 VALUES (201601) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201602 VALUES (201602) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201603 VALUES (201603) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201604 VALUES (201604) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201605 VALUES (201605) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201606 VALUES (201606) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201607 VALUES (201607) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201608 VALUES (201608) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201609 VALUES (201609) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201610 VALUES (201610) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201611 VALUES (201611) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201612 VALUES (201612) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201701 VALUES (201701) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201702 VALUES (201702) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201703 VALUES (201703) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201704 VALUES (201704) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201705 VALUES (201705) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201706 VALUES (201706) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201707 VALUES (201707) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201708 VALUES (201708) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201709 VALUES (201709) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201710 VALUES (201710) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201711 VALUES (201711) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201712 VALUES (201712) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201801 VALUES (201801) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201802 VALUES (201802) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201803 VALUES (201803) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201804 VALUES (201804) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201805 VALUES (201805) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201806 VALUES (201806) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201807 VALUES (201807) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201808 VALUES (201808) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201809 VALUES (201809) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201810 VALUES (201810) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201811 VALUES (201811) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201812 VALUES (201812) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201901 VALUES (201901) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201902 VALUES (201902) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201903 VALUES (201903) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201904 VALUES (201904) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201905 VALUES (201905) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201906 VALUES (201906) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201907 VALUES (201907) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201908 VALUES (201908) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201909 VALUES (201909) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201910 VALUES (201910) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201911 VALUES (201911) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_201912 VALUES (201912) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202001 VALUES (202001) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202002 VALUES (202002) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202003 VALUES (202003) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202004 VALUES (202004) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202005 VALUES (202005) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202006 VALUES (202006) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202007 VALUES (202007) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202008 VALUES (202008) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202009 VALUES (202009) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202010 VALUES (202010) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202011 VALUES (202011) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202012 VALUES (202012) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202101 VALUES (202101) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202102 VALUES (202102) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
,
  PARTITION CHINA_DEMAND_202103 VALUES (202103) 
  NOLOGGING 
  TABLESPACE DWRGDW01_ORDERS 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS FOR QUERY HIGH
)
