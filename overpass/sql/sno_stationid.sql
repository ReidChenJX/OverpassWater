SELECT
	S_NO,
	S_STATIONID,
	( SELECT DISTINCT
			S_NAME
	FROM
			"T_DICTIONARYS_CONFIG"
	WHERE
			lpad( S_DIC_VALUE, 2, '0' ) = S_DIST AND S_NO = 'District' ) AS S_DIST
FROM
	"T_JISHUI"
WHERE
	S_MONTYPE = 'PS1301'