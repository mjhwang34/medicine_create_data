import sys
import time
import pandas as pd
import json
from datetime import datetime, timedelta, timezone
from collections import OrderedDict
import zipfile
import os
from bs4 import BeautifulSoup
from urllib import request
import requests
import csv


#설정
KST = timezone(timedelta(hours=9))
time_record = datetime.now(KST)
today = time_record.strftime('%Y%m%d')

start_date = datetime.now() + timedelta(days=-60)
end_date = datetime.now() + timedelta(days=-30)

excelfile_path = 'C:\\Users\\user\\Medicine\\run\\download'
documentfile_path = 'C:\\Users\\user\\Medicine\\run\\document'
dbfile_path = 'C:\\Users\\user\\Medicine\\run\\db'
csvfile_path = 'C:\\Users\\user\\Medicine\\run\\dbcsv'
chart_excelfile_path = 'C:\\Users\\user\\Medicine\\run\\chart'
chart_db_csvfile_path = 'C:\\Users\\user\\Medicine\\run\\chart_db'

def_db = {'ITEM_SEQ':'품목일련번호', 'ITEM_NAME':'품목명', 'ENTP_NAME':'업체명', 'ITEM_PERMIT_DATE':'허가일자', 'ETC_OTC_CODE':'전문일반', 'CHART':'성상', 'BAR_CODE':'표준코드', 'MATERIAL_NAME':'원료성분', 'EE_DOC_ID':'효능효과', 'UD_DOC_ID':'용법용량', 'NB_DOC_ID':'주의사항', 'STORAGE_METHOD':'저장방법', 'VALID_TERM':'유효기간', 'REEXAM_TARGET':'재심사대상', 'REEXAM_DATE':'재심사기간', 'PACK_UNIT':'포장단위', 'EDI_CODE':'보험코드', 'PERMIT_KIND_NAME':'허가/신고구분', 'MAKE_MATERIAL_FLAG':'완제원료구분', 'NEWDRUG_CLASS_NAME':'신약여부', 'CANCEL_DATE':'취소일자', 'CANCEL_NAME':'취소상태', 'CHANGE_DATE': '변경일자', 'NARCOTIC_KIND_NAME':'마약류분류'}
def_chart = {'ITEM_SEQ':'품목일련번호', 'ITEM_NAME':'품목명', 'ENTP_SEQ':'업소일련번호', 'ENTP_NAME':'업소명', 'CHART':'성상', 'ITEM_IMAGE':'큰제품이미지', 'PRINT_FRONT':'표시앞', 'PRINT_BACK':'표시뒤', 'DRUG_SHAPE':'의약품제형', 'COLOR_CLASS1':'색상앞', 'COLOR_CLASS2':'색상뒤', 'LINE_FRONT':'분할선앞', 'LINE_BACK':'분할선뒤', 'LENG_LONG':'크기장축', 'LENG_SHORT':'크기단축', 'THICK':'크기두께', 'IMG_REGIST_TS':'이미지생성일자(약학정보원)', 'CLASS_NO':'분류번호', 'CLASS_NAME':'분류명', 'ETC_OTC_CODE':'전문일반구분', 'ITEM_PERMIT_DATE':'품목허가일자', 'FORM_CODE_NAME':'제형코드명', 'MARK_CODE_FRONT_ANAL': '표기내용앞', 'MARK_CODE_BACK_ANAL':'표기내용뒤', 'MARK_CODE_FRONT_IMG':'표기이미지앞', 'MARK_CODE_BACK_IMG':'표기이미지뒤', 'MARK_CODE_FRONT':'표기코드앞', 'MARK_CODE_BACK':'표기코드뒤', 'CHANGE_DATE':'변경일자'}
xmlfile_common_path = 'C:\\Users\\user\\Medicine\\run\\unzip\\'
def_xmlfile_folder_path = [xmlfile_common_path+'1954-2000', xmlfile_common_path+'2001-2005', xmlfile_common_path+'2006-2010', xmlfile_common_path+'2011-2015', xmlfile_common_path+'2016-2020']

#엑셀파일 다운로드
def download_excelfile_by_api(today, excelfile_path):
    excelfile_url='https://nedrug.mfds.go.kr/cmn/xls/down/OpenData_ItemPermitDetail'
    excelfile_download_path = excelfile_path + '\\OpenData_ItemPermitDetail'+today+'.xls'

    request.urlretrieve(excelfile_url, excelfile_download_path)
    print("저장되었습니다.")

#엑셀파일 내용 읽기
def load_file(date, path):
    excelfile_load_path = path +'\\OpenData_ItemPermitDetail'+date+'.xls'
    df = pd.read_excel(excelfile_load_path)
    return df

#저장 또는 업데이트 할 내용의 리스트를 반환한다.
def get_list_from_db(save_or_update, df, start_date = '', end_date = '', type = ''):
    if(save_or_update=='save'):
        df1 = df.loc[(df['품목일련번호'] >= 195500000) & (df['품목일련번호'] < (int(today[0:4])+1)*100000)]
    elif(save_or_update=='update'):
        st_day_int = int(start_date.strftime('%Y%m%d'))
        end_day_int = int(end_date.strftime('%Y%m%d'))
        df1 = df.loc[(df[type] >= st_day_int) & (df[type] <= end_day_int)]
    df1 = df1.fillna('')
    print('get_list_from_db end & make_db_json_list start')
    time_record_make_start = datetime.now(KST)
    print(time_record_make_start)
    out_json = make_db_json_list(df1)
    print('make_db_json_list end & get_list_from_db end')
    time_record_make_end = datetime.now(KST)
    print(time_record_make_end)
    return out_json

#{"195500002": {"item_seq": "195500002", "item_name": "종근당염산에페드린정", "entp_name": "(주)종근당", "chart": "본품은 백색의 정제다.", "item_permit_date": "19550117", "edi_code": "", "permit_kind_name": "허가", "cancel_name": "유효기간만료", "cancel_date": "20200101", "etc_otc_code": "전문의약품", "main_item_ingr": "[M040420]염산에페드린", "atc": "R03CA02"}, ... }
def make_db_json_list(df):
    out_json = {}
    check_file_name = []
    for idx in df.index : 
        file_name = get_file_name(str(df['품목일련번호'][idx]))
        item_seq = str(df['품목일련번호'][idx])
        data = get_data_dbjson(idx, df)
        out_json = push_into_out_json(file_name, out_json, item_seq, data)
    return out_json

#dbjson에 넣을 항목에 따라 저장한다
def get_data_dbjson(idx, df):
    data = OrderedDict()  
    for key, value in def_db.items():
        data[key] = str(df[value][idx])
        if(value=='취소일자' or value=='변경일자'):
            data[key] = str(df[value][idx])[0:8]
        if(value=='표준코드'):
            data[key] = str(df[value][idx])[0:13]
    return data

#195500002 -> 1955_00001-00100
def get_file_name(item_seq):
    year = int(item_seq[0:4])
    seq = int(item_seq[4:9])
    temp = int((seq-1)/100)
    range_st = str(temp*100+1)
    range_end = str(temp*100+100)
    five_zeros = '00000'
    range_st = five_zeros[:5-len(range_st)]+range_st
    range_end = five_zeros[:5-len(range_end)]+range_end
    return str(year)+'_'+range_st+'-'+range_end

#파일 저장 또는 업데이트 시작
def save_or_update(out_json, type, file_path):
    file_list = os.listdir(file_path)
    for file_name in out_json:
        print(file_name)
        full_file_name = type+'_'+file_name+".json"
        if file_name in file_list:
            with open(file_path+'/'+full_file_name, 'r', encoding='utf8') as file:
                content = file.read()
            content_json = json.loads(content)
            for item_seq in out_json[file_name]:
                content_json[item_seq] = out_json[file_name][item_seq]
            obj = json.dumps(content_json, ensure_ascii=False)
            time.sleep(0.1)
        else:
            content_json = {}
            for item_seq in out_json[file_name]:
                content_json[item_seq] = out_json[file_name][item_seq]
            obj = json.dumps(content_json, ensure_ascii=False)
            time.sleep(0.1)
            file_list.append(full_file_name)
        with open(file_path+'/'+full_file_name, 'w', encoding='utf8') as file:
            file.write(obj)
            time.sleep(0.1)

#document 파일로부터 out_json 생성
def get_list_from_document(folder_path):
    xmlfile_folder_list = os.listdir(folder_path)
    out_json={}
    for item_seq in xmlfile_folder_list:
        print(item_seq)
        data = {}
        ee = get_content_from_document(folder_path+'/'+item_seq+'/EE_DOC_DATA.xml')
        ud = get_content_from_document(folder_path+'/'+item_seq+'/UD_DOC_DATA.xml')
        nb = get_content_from_document(folder_path+'/'+item_seq+'/NB_DOC_DATA.xml')
        data['EE_DOC_DATA'] = xml_to_json(ee)
        data['UD_DOC_DATA'] = xml_to_json(ud)
        data['NB_DOC_DATA'] = xml_to_json(nb)
        file_name = get_file_name(item_seq)
        out_json = push_into_out_json(file_name, out_json, item_seq, data)
        #print(data)
    return out_json

#document의 xml파일을 읽어 beautifulsoup 형태로 반환
def get_content_from_document(file_path):
    if not os.path.isfile(file_path):
        data = {}
        return data
    with open(file_path, 'r', encoding='utf8') as file:
        contents = file.read()
        soup = BeautifulSoup(contents, 'xml')
    return soup

#api로부터 누락된 시퀀스의 out_json 생성
def get_list_from_api(missing_seq_arr):
    serviceKey = 'idexeeezUsEPdbnJ%2BWNFg1ImgUZ21EIA%2BzhJbHrUn3NA%2FoAzR3YTaTPH2nTXKGuSA%2BxjyemKa81puLL303Yiww%3D%3D'
    out_json = {}
    for item_seq in missing_seq_arr:
        url = 'http://apis.data.go.kr/1471000/DrugPrdtPrmsnInfoService02/getDrugPrdtPrmsnDtlInq01?serviceKey='+serviceKey+'&item_seq='+item_seq
        print(item_seq)
        response = requests.get(url)
        contents = response.text
        soup = BeautifulSoup(contents, 'xml')
        data = {}
        ee = soup.find('EE_DOC_DATA')
        ud = soup.find('UD_DOC_DATA')
        nb = soup.find('NB_DOC_DATA')
        data['EE_DOC_DATA'] = xml_to_json(ee)
        data['UD_DOC_DATA'] = xml_to_json(ud)
        data['NB_DOC_DATA'] = xml_to_json(nb)
        file_name = get_file_name(item_seq)
        out_json = push_into_out_json(file_name, out_json, item_seq, data)
    return out_json

#out_json에서 item_seq의 리스트를 반환한다.
def get_item_seq_list_from_out_json(out_json):
    item_seq_list = []
    for file_name in out_json:
        for item_seq in out_json[file_name]:
            item_seq_list.append(item_seq)
    return item_seq_list

#out_json에 데이터 집어넣기
def push_into_out_json(file_name, out_json, item_seq, data):
    if file_name in out_json:
        out_json[file_name][item_seq] = data
    else:
        out_json[file_name] = {}
        out_json[file_name][item_seq] = data
    return out_json

#beautifulsoup의 xml을 json 형태로 바꾸기
def xml_to_json(content):
    data = []
    if content=={} or content is None:
        return data
    soup=content
    for docElement in soup.find_all('DOC'):
        dataDoc = {}
        dataDoc['title'] = docElement['title']
        dataDoc['sections'] = []
        for sectionElement in soup.find_all('SECTION'):
            dataSection = {}
            dataSection['title'] = sectionElement['title']
            dataSection['articles'] = []
            for articleElement in soup.find_all('ARTICLE'):
                dataArticle = {}
                dataArticle['title'] = articleElement['title']
                dataArticle['paragraphs'] = []
                for paragraphElement in soup.find_all('PARAGRAPH'):
                    if type(paragraphElement.string) is type(None):
                        continue
                    dataParagraph = {}
                    dataParagraph['tag'] = paragraphElement['tagName']
                    dataParagraph['content'] = paragraphElement.string
                    dataArticle['paragraphs'].append(dataParagraph)
                dataSection['articles'].append(dataArticle)
            dataDoc['sections'].append(dataSection)
        data.append(dataDoc)
    return data

#document파일의 정보 중 빠진 item_seq 리스트를 반환한다.
def get_missing_seq(df, documentfile_path):
    missing_seq_list = []
    cur_document_seq_list = []
    item_seq_list = df['품목일련번호'].to_list()
    item_seq_list = list(map(str, item_seq_list))
    documentfile_name_list = os.listdir(documentfile_path)
    for file_name in documentfile_name_list:
        print(file_name)
        with open(documentfile_path+'\\'+ file_name, 'r', encoding='utf8') as file:
            content = file.read()
        content_json = json.loads(content)
        for item_seq in content_json:
            cur_document_seq_list.append(item_seq)
    for item_seq in item_seq_list:
        if not item_seq in cur_document_seq_list:
            missing_seq_list.append(item_seq)
    return missing_seq_list

#json파일을 dataframe object로 바꿔서 반환한다
def json_to_pd(file_path):
    pdObj = pd.read_json(file_path, orient='index', encoding='utf8')
    return pdObj

#컬럼명 리스트, 데이터 리스트를 파라미터로 입력받으면 csv파일을 생성한다
def write_csv(header, data, check):
    with open(csvfile_path+'\\db.csv', 'a', encoding='utf8', newline='') as file:
        writer = csv.writer(file)
        if(check == True):
            check = False
            # write the header
            writer.writerow(header)
        # write multiple rows
        writer.writerows(data)  

#features 엑셀 파일 다운로드
def dowload_chart_excelfile_by_api(today, chart_excelfile_path):
    chart_excelfile_url = "https://nedrug.mfds.go.kr/cmn/xls/down/OpenData_PotOpenTabletIdntfc"
    chart_excelfile_download_path = chart_excelfile_path + '\\chart_'+today+'.xls'
    request.urlretrieve(chart_excelfile_url, chart_excelfile_download_path)
    print("저장되었습니다.")

#features 엑셀 파일을 dataframe으로 반환
def load_chart(today, path):
    excelfile_load_path = path +'\\chart_'+today+'.xls'
    df = pd.read_excel(excelfile_load_path)
    df = df.fillna('')
    return df

#chart 데이터 가공
def chart_data_update(df):
    change_date_list = df['변경일자'].to_list()
    class_no_list = df['분류번호'].to_list()
    change_date_list = list(map(str, change_date_list))
    class_no_list = list(map(str, class_no_list))
    for idx in range(len(change_date_list)):
        change_date_list[idx] = change_date_list[idx][0:8]
        class_no_list[idx] = change_date_list[idx][0:5]
    df['변경일자'] = pd.Series(change_date_list)
    df['분류번호'] = pd.Series(class_no_list)
    return df
#features의 컬럼 값을 영어로 바꿔줌
def chart_column_update(df):
    reversed_def_chart = {v:k for k,v in def_chart.items()}
    column_list = []
    for column in df.columns:
        column_list.append(reversed_def_chart[column])
    df.columns = column_list
    return df

def print_help():
    print('HELP')
    print('최초 전체 db 생성: -a') 
    print('업데이트 추가 & 새로운 허가분에 대한 document 저장: -b')
    print('업데이트 변경일자 : -c')
    print('업데이트 취소일자 : -d')
    print('최초 전체 document 생성: -e')
    print('엑셀파일과 비교했을 때 document에 누락된 item_seq를 찾아내서 api를 요청한 후 documnet 생성: -f')
    print('db.json을 csv파일로 바꿔서 저장: -g')
    print('chart 엑셀 파일 다운로드: -i')
    print('chart excel 파일 csv로 바꿔서 저장: -j')

arguments = sys.argv

if arguments[1] == '-h' or arguments[1] == '-H' or len(arguments)==0:
    print_help()
elif arguments[1]=='-a':
    # 최초 전체 생성
    download_excelfile_by_api(today, excelfile_path)
    df = load_file(today, excelfile_path)
    out_json = get_list_from_db('save', df)
    save_or_update(out_json, 'db', dbfile_path)
elif arguments[1]=='-b':
    #업데이트 - 추가
    download_excelfile_by_api(today, excelfile_path)
    df = load_file(today, excelfile_path)
    out_json = get_list_from_db('update', df, start_date, end_date, '허가일자')
    save_or_update(out_json, 'db', dbfile_path)

    #새로운 허가분에 대한 item_seq의 document 저장
    missing_seq_list = get_item_seq_list_from_out_json(out_json);
    _out_json = get_list_from_api(missing_seq_list)
    save_or_update(_out_json, 'document', documentfile_path)
elif arguments[1]=='-c':
    #업데이트 - 변경일자
    download_excelfile_by_api(today, excelfile_path)
    df = load_file(today, excelfile_path)
    out_json = get_list_from_db('update', df, start_date, end_date, '변경일자')
    save_or_update(out_json, 'db', dbfile_path)
elif arguments[1]=='-d':
    #업데이트 - 취소일자
    download_excelfile_by_api(today, excelfile_path)
    df = load_file(today, excelfile_path)
    out_json = get_list_from_db('update', df, start_date, end_date, '취소일자')
    save_or_update(out_json, 'db', dbfile_path)
elif arguments[1]=='-e':
    #document 생성
    for path in def_xmlfile_folder_path:
        time_record_document_start = datetime.now(KST)
        print('get_list_from_document start')
        print(time_record_document_start)
        out_json = get_list_from_document(path)
        time_record_document_end = datetime.now(KST)
        print('get_list_from_document end & save start')
        print(time_record_document_end)
        save_or_update(out_json, 'document', documentfile_path)
        time_record_save_end = datetime.now(KST)
        print('save end')
        print(time_record_save_end)
elif arguments[1]=='-f':
    #엑셀파일과 비교했을 때 document.json에 누락된 item_seq 저장
    df = load_file(today, excelfile_path)
    missing_seq_list = get_missing_seq(df, documentfile_path)
    print("missing_seq_list")
    print(missing_seq_list)

    #document api를 요청해서 document 생성
    out_json = get_list_from_api(missing_seq_list)
    save_or_update(out_json, 'document', documentfile_path)
elif arguments[1]=='g':
    #db.json을 csv로 바꿈
    dbfile_name_list = os.listdir(dbfile_path)
    check_first = True
    for file_name in dbfile_name_list:
        print(file_name)
        cur_file_path = dbfile_path + '\\' + file_name
        cur_pdObj = json_to_pd(cur_file_path)
        cur_headers_list = cur_pdObj.columns.tolist()
        cur_values_list = cur_pdObj.values.tolist()
        write_csv(cur_headers_list, cur_values_list, check_first)
        check_first = False
elif arguments[1]=='i':
    #chart excel 파일 다운로드
    dowload_chart_excelfile_by_api(today, chart_excelfile_path)
elif arguments[1]=='j':
    #chart excel 파일 csv로 바꾸기
    df = load_chart(today, chart_excelfile_path)
    df = chart_data_update(df)
    df = chart_column_update(df)
    df.to_csv(chart_db_csvfile_path+'\\chart_'+today+'.csv', mode='w', encoding='utf8', index=False)
