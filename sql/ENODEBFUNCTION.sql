CREATE TABLE ERIC_45G_ENODEBFUNCTION
(
BEGINTIME DATE,
ELEMENT VARCHAR2(100 BYTE),
ENDTIME DATE,
UTRANCELL VARCHAR2(100 BYTE),
pmLic5MHzSectorCarrierActual VARCHAR2(250 BYTE),
pmLic5Plus5MHzScFddActual VARCHAR2(250 BYTE),
pmLic5Plus5MHzScTddActual VARCHAR2(250 BYTE),
pmLicConnectedUsersActual VARCHAR2(250 BYTE),
pmLicConnectedUsersDistr VARCHAR2(250 BYTE),
pmLicConnectedUsersLevSamp VARCHAR2(250 BYTE),
pmLicConnectedUsersLevSum VARCHAR2(250 BYTE),
pmLicConnectedUsersLicense VARCHAR2(250 BYTE),
pmLicConnectedUsersMax VARCHAR2(250 BYTE),
pmLicConnectedUsersTimeCong VARCHAR2(250 BYTE),
pmLicDlCapLicense VARCHAR2(250 BYTE),
pmLicDlPrbCapLicense VARCHAR2(250 BYTE),
pmLicUlCapLicense VARCHAR2(250 BYTE),
pmLicUlPrbCapLicense VARCHAR2(250 BYTE),
pmMoFootprintMax VARCHAR2(250 BYTE),
pmRrcConnBrEnbLevSamp VARCHAR2(250 BYTE),
pmRrcConnBrEnbLevSum VARCHAR2(250 BYTE),
pmRrcConnBrEnbMax VARCHAR2(250 BYTE),
DATETIME_INS TIMESTAMP (6),
DATETIME DATE
)
NOCACHE
  MONITORING
  PARTITION BY RANGE (DATETIME)
  (
  PARTITION p20180406 VALUES LESS THAN (TO_DATE('2018-04-06 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
  PCTFREE     10
  INITRANS    1
  MAXTRANS    255
  LOGGING
  )
/