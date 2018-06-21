__AUTHOR__='Lifecell OSS group'
__COPYRIGHT__='Lifecell UA Company, 2018 Kiev, Ukraine'
__version__ = '0.26'
__license__ = "GPL"
__email__ = "oss_group@lifecell.com.ua"
__status__ = "Pre-Production"
###############################################################################
# Description : This script is used to process the Ericsson LTE  parser (and might be Huawei with small changes)
###############################################################################


import os.path
import re
import csv
import datetime
from collections import OrderedDict
from subprocess import check_call, CalledProcessError

try:    # use C-compiled module for python 2.7 (3.3 will do that by default)
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

try:    #for the old python versions like as on the Optima hosts with Python 2.6.6
    csv.DictWriter.write_header
except AttributeError:
    csv.DictWriter.writeheader = (
        lambda dw: dw.writer.writerow(dw.fieldnames)
        )

try:    # to load user-defined module with ctl and sql capabilities
    import ctl_sql_module
except ImportError, e:
    print('Error with importing ctl_module has occured', e)


def savetoFILE(filename, record, record2='', record3='', record4=''):
    """ Function saves data to the specified file """
    if os.path.exists(filename):
        with open(filename, 'a') as f:
            f.write(record + ' ' + record2 + ' ' + record3 + ' ' + record4 + "\n")
    elif not os.path.exists(filename):
        with open(filename, 'w+') as f:
            f.write(record + ' ' + record2 + ' ' + record3 + ' ' + record4 + "\n")
    else:
        print('unknown error occurs at saveFILE function!')


def add_elem_time_and_sort(pm_d_result, site, meas_object, beginTime, endTime):
    """ Function adds four names with values to the dict """
    pm_d_result.update({'ELEMENT': site, 'UTRANCELL': meas_object, 'BEGINTIME': beginTime, 'ENDTIME': endTime})  # we can assign any names in titles of the csv file here
    #print('pm_d_result: ', pm_d_result, 'type: ', type(pm_d_result))
    sorted_dict = OrderedDict(sorted(pm_d_result.items(), key=lambda t: t[0]))  # we must to sort DICT by KEYS
    #print('Sorted pm_d_result: ', sorted_dict, 'type: ', type(sorted_dict))
    return sorted_dict


def get_begin_end_time(filename, manag_elem, abs_log_filename='', pm_group=''):
    """ Function gets beginTime and endTime from the filename """
    ropPeriod = 15  # Measuremnt perid equals 15 mins for 3G and 4G
    match = re.search(r'^.*MeContext=(ERBS_\w+\d+).*$', filename)
    if match:
        site = match.group(1)
    if site <> manag_elem:
        message = 'manag_elem from filename is different into the xml file :', filename, 'manag_elem: ', manag_elem, 'site: ', site
        print(message)  # additional checking
        savetoFILE(abs_log_filename, pm_group, message)
    try:
        year = int(filename[1:5])
        month = int(filename[5:7])
        day = int(filename[7:9])
        beg_hour = int(filename[10:12])
        min = int(filename[12:14])
        gmt = int(filename[15:17])
    except ValueError as e:
        message = 'Error in convertation year, month, day, beg_hour, min, gmr to int type: ' + str(e)
        print(message)
        savetoFILE(abs_log_filename, pm_group, message)

    date = datetime.date(year, month, day)
    d = datetime.datetime(year, month, day, beg_hour, min)
    beginTime = d - datetime.timedelta(hours=gmt)  #('beginTime: ', datetime.datetime(2018, 3, 19, 8, 0))
    endTime = beginTime + datetime.timedelta(minutes=ropPeriod)
    beginTime_str = str(beginTime)  #'beginTime_str', '2018-03-19 08:00:00'
    endTime_str = str(endTime)  #'2018-03-19 08:15:00'
    #print('beginTime: ', beginTime, 'beginTime_str', beginTime_str, 'endTime', endTime, 'endTime_str', endTime_str)
    #print('year: ', year, 'month: ', month, 'day: ', day)  #str: ('year: ', '2018', 'month: ', '03', 'day: ', '19')  int: ('year: ', 2018, 'month: ', 3, 'day: ', 19)
    #print('beg_hour: ', beg_hour, 'min: ', min, 'gmt: ', gmt)
    return manag_elem, beginTime_str, endTime_str


def savetoCSV(pm_d_result, filename):
    """ Save parsed data to csv file. One PM group equals one csv file without dependencies on quantities of files"""
    fields = pm_d_result.keys()  # specifying the fields for csv file
    if os.path.exists(filename):
        with open(filename, 'a') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields, delimiter=';', lineterminator='\n')
            #writer.writeheader()  # we dont' need header more than 1 time
            writer.writerow(pm_d_result)
    elif not os.path.exists(filename):
        with open(filename, 'w+') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields, delimiter=';', lineterminator='\n')
            writer.writeheader()
            writer.writerow(pm_d_result)
    else:
        print('unknown error occurs!')


def parseXML(xmlfile, pm_group):
    """ PM_GROUP parser """
    with open(xmlfile, 'rt') as f:  ## open xml file for parsing
        try:
            tree = ET.parse(f)
            root = tree.getroot()
        except:
            print('It is unknown exception raised during xml parsing by ET module. The failed xml file: ', xmlfile)


    for child_of_root in root:   #1st Level
        parse = False  # First initialization
        #line = 'tag1: ', child_of_root.tag, 'attrib1: ', child_of_root.attrib, 'keys1: ', child_of_root.keys(), 'items1: ', child_of_root.items(), 'text1: ', child_of_root.text
        #print(line)

        if 'dnPrefix' in child_of_root.attrib:
            manag_elem = child_of_root.attrib['dnPrefix']
            pos = manag_elem.find('ERBS_') #looking for a position of ERBS* 35
            if pos:
                manag_elem = manag_elem[pos:]# to catch exactly element name starts with ERBS_
                #print('manag_elem: ', manag_elem)
            else:
                print('pos is None type')

        for node in child_of_root:          # 2nd Level
            parse = False                   # Control one more time

            for item in node.items():
                parse = False  # Control one more time
                pm = 'PM=1,PmGroup=' + pm_group
                #print('pm: ', pm)

                if item[1] == pm:
                    parse = True  # Caught the target PM group
                    #print('The loop is started.')
                    #print('PmGroup= ', pm_group)

            if parse:
                #line = 'tag2: ', node.tag, 'attrib2: ', node.attrib, 'keys2: ', node.keys(), 'items2: ', node.items(), 'text2: ', node.text
                #print(line)

                pm_name_dict = {}  #'60': 'pmRrcConnEstabAttDta'
                for elem in node:  #3rd Level
                    a = elem.items()  # type list
                    b = [i for sub in a for i in
                         sub]  # to join a list of tuples into one list  [('p', '19')]  --> ['p', '19']
                    key, value = b[1], elem.text  # to assign number as key and pm_name as values
                    #if value is not None:
                    #if len(value) < 30:     #Protection from ORA-00972: identifier is too long - Action: Specify at most 30 characters (in Oracle v. < 12.2)
                    pm_name_dict.update({key: value}) # {'[p]1': 'pmLic5MHzSectorCarrierActual'}
                    #print('key, value :', key, value)
                    #key, value = b[1], elem.text  # to assign number as key and pm_name as values
                    ##if key.isdigit():  # omit all except key is numeric - can not catch measObjLdn key values
                    #line = 'tag3: ', elem.tag, 'attrib3: ', elem.attrib, 'keys3: ', elem.keys(), 'items3: ', elem.items(), 'text3: ', elem.text
                    #print(line)

                    if 'measObjLdn' in elem.attrib:
                        manag_obj = elem.attrib['measObjLdn']
                        if pm_group.upper() == 'EUTRANCELLFDD': #EUtranCellFDD' EUTRANCELLFDD
                            #if(str.contains("="))
                            match = re.search(r'^ManagedElement=.*EUtranCellFDD=(\w+?\d+_?\d+?_?\w+),.*$', manag_obj)  #HORIZ12_7_L21
                            #string = ^((.*?)(whatever))*(.*)$
                            #ManagedElement=ERBS_KI0077,ENodeBFunction=1,EUtranCellFDD=ERBS_KI0077L13
                            if match:
                                manag_obj = match.group(1)
                                #print('manag_obj: ', manag_obj)
                            else:
                                print('manag_obj: Nothing was found', manag_obj)
                                manag_obj = 'ERBS_XXXXX_Y_Z'
                                #{'measObjLdn': 'ManagedElement=ERBS_KI0195,ENodeBFunction=1,EUtranCellFDD=ERBS_KI0195L34'}

                    pm_values_dict = {}  #{'60': '0', '61': '0'...
                    for each in elem:   #4th Level
                        a = each.items()  # <type 'list'>: [('p', '1')] elem.text
                        b = [i for sub in a for i in sub]
                        key, value = b[1], each.text  # to assign normal names to them :)
                        #print('key, value :', key, value)
                        pm_values_dict.update({key: value})  # {'p1': '12'}
                        #line = 'tag4: ', each.tag, 'attrib4: ', each.attrib, 'keys4: ', each.keys(), 'items4: ', each.items(), 'text4: ', each.text
                        #line = 'attrib4: ', value.attrib, 'keys4: ', value.keys(), 'items4: ', value.items(), 'text4: ', value.text
                        #line 130, in parseXML  - AttributeError: 'str' object has no attribute 'tag'
                        #print(line)

                pm_d_result = {}  # merge 2 dicts like as  {'pmS1SigConnEstabSuccMod': '0', 'pmUeCtxtRelMmeAct': '0'
                for key in pm_name_dict:
                    if key in pm_values_dict:
                        pm_d_result[pm_name_dict[key]] = pm_values_dict[key]
                #print('pm_d_result: ', pm_d_result)
                return pm_d_result, manag_elem, manag_obj


def main():
    """ It must be created 6 sub-folders at main execution folder: xml, csv, log, list, crt, sql """
    cred = "NETWORK_BSS\@OPTIMA7/NETXXXXXX"  #credentials for sqlldr
    #pm_groups = ['ENodeBFunction']
    #pm_groups = ['EUtranCellFDD']
    #pm_groups = ['EUTRANCELLFDD']
    pm_groups = ['ENodeBFunction', 'EUtranCellFDD', 'EUtranCellRelation']

    if os.getenv('ORACLE_HOME'):
        ORACLE_HOME = os.getenv('ORACLE_HOME')
    else:                                       # if system variables is not defined you must set such parameter here below
        if os.name == 'posix':
            ORACLE_HOME = os.getenv('/u01/app/oracle/product/11.2.0/client_1')
        elif os.name == 'nt':
            ORACLE_HOME = 'C:\\oracle\client\\product\\12.2.0\\client_1'
    #print('ORACLE_HOME: ', ORACLE_HOME)
    #if os.getenv('PATH'):
    #    print(os.getenv('PATH'))

    os.chdir(os.path.dirname(os.path.realpath(__file__)))  #cd to the script execution directory

    dir_xml_path = os.getcwd() + os.sep + 'xml' + os.sep
    dir_csv_path = os.getcwd() + os.sep + 'csv' + os.sep
    dir_log_path = os.getcwd() + os.sep + 'log' + os.sep
    dir_list_path = os.getcwd() + os.sep + 'list' + os.sep
    dir_ctl_path = os.getcwd() + os.sep + 'ctl' + os.sep
    dir_sql_path = os.getcwd() + os.sep + 'sql' + os.sep
    abs_ctl_ldr = ORACLE_HOME + os.sep + 'bin' + os.sep + 'sqlldr' # + '.exe'
    dirs = [dir_xml_path, dir_csv_path, dir_log_path, dir_list_path, dir_ctl_path, dir_sql_path]

    if not all([os.path.isdir(d) for d in dirs]):
        print('at least one directory doesnt exists, create them manually: ', dirs)
        exit(1) # terminates the program if subfolders are missed

    files = os.listdir(dir_xml_path)
    files_xml_list = [i for i in files if i.endswith('.xml')]
    file_count = 0
    #ctl_count = 0 # for the loop os.path.exists(ctl_pmgroup_filename): and module ctl_module

    for filename_xml in files_xml_list:     #ERBS: /var/opt/ericsson/nms_umts_pms_seg/segment1/XML/SubNetwork=ONRM_RootMo_R
        today = datetime.date.today().strftime('%Y%m%d')  #('today: ', '20180404')
        today2 = datetime.date.today().strftime('%Y-%m-%d')
        time = datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')  # '30.03.2018 14:47:00'
        abs_log_filename = dir_log_path + today + '.log'
        file_count += 1
        if file_count == 1:
            message = 'The programm start time is: ' + str(time)
            print(message)
            savetoFILE(abs_log_filename, message)
        abs_xml_filename = dir_xml_path + filename_xml
        abs_list_filename = dir_list_path + 'lastfiles.' + today
        savetoFILE(abs_list_filename, filename_xml)  # lastfiles.20180330 listfile

        for pm_group in pm_groups:
            abs_csv_filename = dir_csv_path + pm_group.upper() + '.csv'
            ctl_pmgroup_filename = dir_ctl_path + pm_group.upper() + '.ctl'
            sql_pmgroup_filename = dir_sql_path + pm_group.upper() + '.sql'
            try:
                pm_d_result,  manag_elem, utrancell = parseXML(abs_xml_filename, pm_group)
            except TypeError as e:
                print('TypeError occurs', e)
                savetoFILE(abs_log_filename, time, manag_elem, pm_group, str(e))
                continue

            site, beginTime, endTime = get_begin_end_time(filename_xml, manag_elem, abs_log_filename, pm_group)    # get element, beginTime, endTime from filename_xml
            pm_d_result = add_elem_time_and_sort(pm_d_result, site, utrancell, beginTime, endTime)                 # add additional items above to dictionary
            savetoCSV(pm_d_result, abs_csv_filename)                                                               # store dict items in a csv file with pm_group as a filename

            # code below for autocreation of ctl files for the particulat specified pmgroup
            if not os.path.exists(ctl_pmgroup_filename):  #only in case if ctl file doesn't exists
                ctl_sql_module.save_ctl(ctl_pmgroup_filename, pm_d_result, abs_csv_filename, pm_group)

            # comment below code if you don't want to make sql files for Oracle tables
            if not os.path.exists(sql_pmgroup_filename):
                ctl_sql_module.save_sql(sql_pmgroup_filename, pm_d_result, pm_group, today, today2)

    # loading data from csv files to Oracle DB by sqlldr utility
    """
    files = os.listdir(dir_ctl_path)
    files_ctl_list = [i for i in files if i.endswith('.ctl')]
    #file_count = 0
    for filename_ctl in files_ctl_list:
        #file_count += 1
        abs_filename_ctl = dir_ctl_path + filename_ctl
        control_file = 'control=' + abs_filename_ctl
        try:
            check_call(["sqlldr.exe", "NETWORK_BSS@OPTIMA7/NETWORKU46", control_file])
        except CalledProcessError, e:
            print('Error occured in call the sqlldr for the file: ', control_file, time)
            savetoFILE(abs_log_filename, 'Error occured in call sqlldr for the file: ', control_file, time, str(e.output))
    """


    #end of sript work. saves the result data to log file
    message = 'The programm finish time is: ' + str(time) + '. It were processed: ' + str(file_count) + ' files ' + 'and ' + str(len(pm_groups)) + ' pm groups'
    print(message)
    savetoFILE(abs_log_filename, message)


if __name__ == "__main__":
    main()
