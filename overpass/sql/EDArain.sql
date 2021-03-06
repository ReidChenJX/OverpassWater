SELECT
	ST_STATIONID,
	DT_TIME,
	NM_RAINVALUE
FROM
	SWCORE.TB_YULIANG_5MIN
WHERE
	DT_TIME >= TO_DATE( '{start_time}', 'YYYY-MM-DD HH24:MI:SS' )
	AND DT_TIME < TO_DATE( '{end_time}', 'YYYY-MM-DD HH24:MI:SS' )
	AND ST_STATIONID='{s_stationid}'