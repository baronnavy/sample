import glob
import pandas as pd
import numpy as np
import os

from multiprocessing import Pool

MODEL_FILE = 'git/device.csv'
INPUT = './'
OUTPUT = './'

DTYPE1 = {'DEVICE_ID':str}
DTYPE2 = {'DEVICE_ID':str}


def read_device():
    '''デバイスIDとモデル情報の辞書作成
    '''
    df = pd.read_csv(MODEL_FILE, index_col='DEVICE_ID', encoding='shift_jis')
    df = df['NUMBER']
    device_dict = df.to_dict()
    return device_dict


def serch_file(device_list, n):
    '''読み込むファイルリストの取得
    '''
    file_list = []
    for device in device_list:
        serch_str = f'{INPUT}' + device + '*' + f'{n}' + '.csv'
        _file_list = glob.glob(serch_str)
        file_list.extend(_file_list)
    return file_list


def readcsv_multi(file_list, n):
    '''ファイルリストのcsv読み込み（並列処理）
    '''
    p = Pool(os.cpu_count())
    if n == 1:
        df = pd.concat(p.map(pdreadcsv1, file_list))
    elif n == 2:
        df = pd.concat(p.map(pdreadcsv2, file_list))
    p.close()
    return df


def pdreadcsv1(csv_path):
    '''type1 csv読み込み（map関数用）
    '''
    df = pd.read_csv(csv_path, dtype=DTYPE1, parse_dates=['TIMESTAMP'], encoding='utf_8')
    return df


def pdreadcsv2(csv_path):
    '''type2 csv読み込み（map関数用）
    '''
    df = pd.read_csv(csv_path, dtype=DTYPE2, parse_dates=['TIMESTAMP'], encoding='utf_8')
    return df


def main():

    # デバイスIDとモデル情報を紐付けた辞書を作成
    device_dict = read_device()
    # デバイスIDのリストを作成
    device_list_all = list(device_dict.keys())
    # デバイスIDのリストをn台ずつに分割
    n = 100

    df_1to1K = pd.DataFrame()
    df_jam = pd.DataFrame()
    df_merge = pd.DataFrame()

    for i in range(0, len(device_list_all), n):
        
        device_list = device_list_all[i: i+n]
        print(device_list)
        # 1to1Kデータの読み込み
        file_list = serch_file(device_list, 1)
        if len(file_list) != 0:
            df_1to1K = readcsv_multi(file_list, 1)
            df_1to1K['DATE'] = df_1to1K.TIMESTAMP.dt.strftime('%Y-%m-01')
            df_serial = df_1to1K.loc[df_1to1K.groupby(['DEVICE_ID','DATE'])['TIMESTAMP'].idxmin(),:]
            print(file_list)
        # JAMデータの読み込み
        file_list = serch_file(device_list, 2)
        if len(file_list) != 0:
            df_jam = readcsv_multi(file_list, 2)
            df_jam['DATE'] = df_jam.TIMESTAMP.dt.strftime('%Y-%m-01')
            print(file_list)
        # ミスプリデータの読み込み

        # EventHistoryデータの読み込み

        # EventCountデータの読み込み

        # ログタイプ別データの結合
        df = pd.concat([df_1to1K, df_jam])

        # 不要なデータフレームの削除
        del df_1to1K,df_jam

        # エラー発生時のインデックスリストの作成
        #_df

        # 集計データの計算
        #df_agg = 

        # 分割データの結合
        df_merge = pd.concat([df_merge, df])

        # 集計データの結合
        #df_agg_merge =


    print(df_merge)

    # csv出力
    #df_merge.to_csv
    #df_agg_merge.to_csv
        
if __name__ == '__main__':
    main()