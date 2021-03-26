INSERT INTO APPL_DT_CORR
SELECT T0.IMC_NO_CORR, T0.OPER_AFF_KEY_NO, T1.CURR_APPL_DT_KEY_NO FROM
(SELECT COALESCE(IMC_NO, IMC_KEY_NO) AS IMC_NO_CORR, OPER_AFF_KEY_NO FROM DWSEAI01.DEMAND_SALES_DET_RAW
GROUP BY COALESCE(IMC_NO, IMC_KEY_NO), OPER_AFF_KEY_NO HAVING COUNT(DISTINCT CURR_APPL_DT_KEY_NO)>1) T0 LEFT JOIN 
(SELECT IMC_NO, IMC_CNTRY_KEY_NO, IMC_AFF_KEY_NO, CURR_APPL_DT_KEY_NO FROM DWSAVR02.DWV01021_IMC_MASTER_DIM WHERE IMC_AFF_KEY_NO IN (17,19,42,39,30,12)) T1
ON IMC_NO_CORR=T1.IMC_NO AND T0.OPER_AFF_KEY_NO=T1.IMC_AFF_KEY_NO;
COMMIT;


MERGE INTO DWSEAI01.DEMAND_SALES_DET_RAW T0
USING (SELECT IMC_NO_CORR, CURR_APPL_DT_KEY_NO, OPER_AFF_KEY_NO FROM APPL_DT_CORR) T1
ON (COALESCE(T0.IMC_NO, T0.IMC_KEY_NO)=T1.IMC_NO_CORR AND T0.OPER_AFF_KEY_NO=T1.OPER_AFF_KEY_NO)
WHEN MATCHED THEN
UPDATE SET T0.CURR_APPL_DT_KEY_NO=T1.CURR_APPL_DT_KEY_NO