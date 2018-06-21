###############################################################################
# Description : This module is a part of the main script which is used to process the Ericsson LTE  parser
# The module ctl_sql_module which generates ctl and sql files must be imported from the main() function
###############################################################################
__AUTHOR__='Lifecell OSS group'
__COPYRIGHT__='Lifecell UA Company, 2018 Kiev, Ukraine'
__version__ = '0.26'
__license__ = "GPL"
__email__ = "ossXXXXX@lifeXXXX.com.ua"
__status__ = "Pre-Production"


def save_ctl(filename, pm_d_result, abs_csv_filename, pm_group):
    """ Save parsed data to csv file. One PM group equals one csv file without dependencies on quantities of files"""
    #print('The function from ctl_module is started !!')
    #'pm_d_result.keys(): ', ['pmRimAssocMax', 'pmMoFootprintMax',
    if len(pm_group) >= 30:
        pm_group = pm_group[:29]  #Protection from ORA-00972: identifier is too long - Action: Specify at most 30 characters (in Oracle v. < 12.2)
    with open(filename, 'w+') as ctl_file:
        ctl_file.writelines('OPTIONS (SKIP=1)' + '\n')
        ctl_file.writelines('LOAD DATA' + '\n')
        ctl_file.writelines('    INFILE ' + '\'' + abs_csv_filename + '\'' + '\n')
        ctl_file.writelines('APPEND' + '\n')
        ctl_file.writelines('INTO TABLE NETWORK_BSS.ERIC_45G_' + pm_group.upper() + '\n')
        ctl_file.writelines('FIELDS TERMINATED BY \';\'' + '\n')
        ctl_file.writelines('TRAILING NULLCOLS' + '\n')
        ctl_file.writelines('(' + '\n')  # start column
        for each_pm in pm_d_result.keys():
            if len(each_pm) >= 30:
                each_pm = each_pm[:29]  # protection from ORA-00972: identifier is too long - Action: Specify at most 30 characters
            if each_pm == 'BEGINTIME':
                ctl_file.writelines(each_pm + ' \"TO_DATE(:BEGINTIME,\'YYYY-MM-DD HH24:MI:SS\')\",' + '\n')
            elif each_pm == 'ENDTIME':
                ctl_file.writelines(each_pm + ' \"TO_DATE(:ENDTIME,\'YYYY-MM-DD HH24:MI:SS\')\",' + '\n')
            else:
                ctl_file.writelines(each_pm + ',' + '\n')
        ctl_file.writelines('DATETIME_INS' + ' ' + 'EXPRESSION \"CAST (SYSTIMESTAMP AS TIMESTAMP (6))\",' + '\n')
        ctl_file.writelines(
            'DATETIME ' + '\"TRUNC(FROM_TZ(CAST(TO_DATE(:BEGINTIME, \'YYYY-MM-DD HH24:MI:SS\') AS TIMESTAMP), \'UTC\') AT TIME ZONE \'EUROPE/KIEV\', \'MI\')\"' + '\n')
        ctl_file.writelines(')' + '\n')  # finish column


def save_sql(filename, pm_d_result, pm_group, today, today2):
    """ To create sql file of appropriated DB table for the corresponding pm_group"""
    if len(pm_group) >= 30:
        pm_group = pm_group[:29]  #Protection from ORA-00972: identifier is too long - Action: Specify at most 30 characters (in Oracle v. < 12.2)
    with open(filename, 'w+') as sql_file:
        sql_file.writelines('CREATE TABLE ERIC_45G_' + pm_group.upper() + '\n')
        sql_file.writelines('(' + '\n')
        for each_pm in pm_d_result.keys():
            if len(each_pm) >= 30:
                each_pm = each_pm[:29]  # protection from ORA-00972: identifier is too long - Action: Specify at most 30 characters
            if each_pm == 'BEGINTIME' or each_pm == 'ENDTIME':
                sql_file.writelines(each_pm + ' DATE,' + '\n')
            elif each_pm == 'ELEMENT' or each_pm == 'UTRANCELL':
                sql_file.writelines(each_pm + ' VARCHAR2(100 BYTE),' + '\n')
            else:
                sql_file.writelines(each_pm + ' VARCHAR2(250 BYTE),' + '\n')
        sql_file.writelines('DATETIME_INS TIMESTAMP (6),' + '\n')
        sql_file.writelines('DATETIME DATE' + '\n')
        sql_file.writelines(')' + '\n')
        sql_file.writelines('NOCACHE' + '\n')
        sql_file.writelines('  MONITORING' + '\n')
        sql_file.writelines('  PARTITION BY RANGE (DATETIME)' + '\n')
        sql_file.writelines('  (' + '\n')
        #sql_file.writelines(
        #    '  PARTITION p' + today + ' VALUES LESS THAN (TO_DATE(\'2018-04-02 00:00:00\', \'SYYYY-MM-DD HH24:MI:SS\', \'NLS_CALENDAR=GREGORIAN\'))' + '\n')
        sql_file.writelines(
            '  PARTITION p' + today + ' VALUES LESS THAN (TO_DATE(\'' + today2 +  ' 00:00:00\', \'SYYYY-MM-DD HH24:MI:SS\', \'NLS_CALENDAR=GREGORIAN\'))' + '\n')
        sql_file.writelines('  PCTFREE     10' + '\n')
        sql_file.writelines('  INITRANS    1' + '\n')
        sql_file.writelines('  MAXTRANS    255' + '\n')
        sql_file.writelines('  LOGGING' + '\n')
        sql_file.writelines('  )' + '\n')
        sql_file.writelines('/' + '\n')


def main():
    print('That is only a module. You don\'t need to start it separately. Exiting....')
    exit(1)


if __name__ == "__main__":
    main()
