OPTIONS (SKIP=1)
LOAD DATA
    INFILE 'C:\Users\vdanevyc\PycharmProjects\xml_learning\python_docs\csv\ENODEBFUNCTION.csv'
APPEND
INTO TABLE NETWORK_BSS.ERIC_45G_ENODEBFUNCTION
FIELDS TERMINATED BY ';'
TRAILING NULLCOLS
(
BEGINTIME "TO_DATE(:BEGINTIME,'YYYY-MM-DD HH24:MI:SS')",
ELEMENT,
ENDTIME "TO_DATE(:ENDTIME,'YYYY-MM-DD HH24:MI:SS')",
UTRANCELL,
pmLic5MHzSectorCarrierActual,
pmLic5Plus5MHzScFddActual,
pmLic5Plus5MHzScTddActual,
pmLicConnectedUsersActual,
pmLicConnectedUsersDistr,
pmLicConnectedUsersLevSamp,
pmLicConnectedUsersLevSum,
pmLicConnectedUsersLicense,
pmLicConnectedUsersMax,
pmLicConnectedUsersTimeCong,
pmLicDlCapLicense,
pmLicDlPrbCapLicense,
pmLicUlCapLicense,
pmLicUlPrbCapLicense,
pmMoFootprintMax,
pmRrcConnBrEnbLevSamp,
pmRrcConnBrEnbLevSum,
pmRrcConnBrEnbMax,
DATETIME_INS EXPRESSION "CAST (SYSTIMESTAMP AS TIMESTAMP (6))",
DATETIME "TRUNC(FROM_TZ(CAST(TO_DATE(:BEGINTIME, 'YYYY-MM-DD HH24:MI:SS') AS TIMESTAMP), 'UTC') AT TIME ZONE 'EUROPE/KIEV', 'MI')"
)
