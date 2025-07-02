# anomaly_detection_system.py
# 파트너, 이 스크립트는 빗썸 API에서 데이터를 수집하고 이상거래를 탐지합니다.
# 수집된 데이터와 이상 징후 로그는 'shared_data.json' 파일에 저장되어
# 웹 서버와 공유됩니다. 이제 '스푸핑' 탐지 로직이 추가됩니다.
# **SyntaxError: expected 'except' or 'finally' block 오류 해결을 위해 코드 구조와 들여쓰기를 재확인합니다.**

import requests
import time
import os
import pandas as pd
from datetime import datetime, timedelta
import traceback
import subprocess
import sys
import json

# --- 설정 (Configuration) ---
BASE_URL = "https://api.bithumb.com/public"
TICKER_ENDPOINT = "/ticker/ALL_KRW"
TICKER_REQUEST_URL = BASE_URL + TICKER_ENDPOINT

TARGET_COINS = ["BTC", "ETH", "DOGE"] # 감시할 코인 목록

CSV_FILE_PATH = "bithumb_ticker_data.csv" # 데이터 저장 CSV 파일 경로 (이력 관리용)
LOG_FILE_PATH = "anomaly_detection_log.log" # 콘솔 출력 로그 파일 경로 (디버깅용)
SHARED_DATA_FILE = "shared_data.json" # 웹 서버와 공유할 데이터 파일

# 웹 UI에 표시할 데이터 이력 길이 (예: 5분치 데이터, 5초마다 업데이트 시 60개)
DATA_HISTORY_LENGTH = 60

# --- 전역 변수 (Global Variables for Shared Data) ---
# 이 변수들은 주기적으로 JSON 파일에 저장됩니다.
global_coin_data_history = {coin: [] for coin in TARGET_COINS}
global_anomaly_logs = [] # 웹 UI에 표시할 이상 징후 로그
global_current_summary = {coin: {} for coin in TARGET_COINS} # 웹 UI에 표시할 최신 요약 정보

# 스푸핑 탐지를 위해 직전 주기에서 감지된 대규모 호가벽 정보를 저장합니다.
# {'coin_symbol': [{'price': float, 'quantity': float, 'type': 'ask'/'bid', 'timestamp': datetime}, ...]}
global_last_detected_large_walls = {coin: [] for coin in TARGET_COINS}


# 프로그램 시작 시 기존 공유 데이터 로드
if os.path.exists(SHARED_DATA_FILE):
    try:
        with open(SHARED_DATA_FILE, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
            if isinstance(loaded_data.get('history'), dict):
                global_coin_data_history = loaded_data['history']
            if isinstance(loaded_data.get('anomaly_logs'), list):
                global_anomaly_logs = loaded_data['anomaly_logs']
            if isinstance(loaded_data.get('current_summary'), dict):
                global_current_summary = loaded_data['current_summary']
            # 이전 스푸핑 관련 데이터도 로드 시도 (없으면 빈 리스트로 초기화)
            if isinstance(loaded_data.get('last_detected_large_walls'), dict):
                global_last_detected_large_walls = loaded_data['last_detected_large_walls']
            print(f"기존 공유 데이터 '{SHARED_DATA_FILE}' 로드 완료.")
    except Exception as e:
        print(f"공유 데이터 '{SHARED_DATA_FILE}' 로드 실패: {e}. 새 데이터로 시작합니다.")

# 기존 이상 징후 감지 설정
FLUCTUATION_THRESHOLD_PERCENT = 5.0
MOVING_AVERAGE_WINDOW = 10
DEVIATION_THRESHOLD_PERCENT = 1.0
WASH_TRADE_TIME_WINDOW_SECONDS = 2
WASH_TRADE_PRICE_TOLERANCE_PERCENT = 0.05
WASH_TRADE_QUANTITY_TOLERANCE_PERCENT = 0.1
RECENT_TRADES_LOOKBACK_COUNT = 50
ORDER_BOOK_COUNT = 10
ORDER_WALL_VOLUME_MULTIPLIER = 2.0
ORDER_WALL_PRICE_DISTANCE_PERCENT = 0.5
PUMP_DETECTION_WINDOW_SECONDS = 300
PUMP_PRICE_INCREASE_PERCENT = 3.0
PUMP_VOLUME_MULTIPLIER_FROM_AVG_24H = 5.0

API_CALL_INTERVAL_SECONDS = 5 # API 호출 및 전체 분석 주기 (초 단위)

# --- 함수 정의 (Function Definitions) ---

# 모든 print() 출력을 파일과 콘솔 모두에 기록하도록 오버라이드
# is_anomaly=True 일 경우 웹 UI용 로그에도 추가
def print_and_log(message, file_path=LOG_FILE_PATH, is_anomaly=False):
    print(message)
    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(message + "\n")
            f.flush() # 즉시 파일에 쓰기 (버퍼링 방지)
    except IOError as e:
        print(f"로그 파일 '{file_path}' 쓰기 오류: {e}")
    
    # 웹 UI용 로그에 추가 (이상 징후만 추가)
    if is_anomaly:
        global global_anomaly_logs
        # 로그에 타임스탬프 추가
        global_anomaly_logs.append(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")
        # 로그가 너무 많아지지 않도록 일정 길이 유지
        if len(global_anomaly_logs) > 100: # 최대 100개 로그 유지
            global_anomaly_logs = global_anomaly_logs[-100:]

# CSV 파일 헤더 작성 함수
def write_csv_header_if_not_exists(file_path, coins):
    header_parts = ["Timestamp (KST)"]
    for coin in coins:
        header_parts.append(f"{coin}_closing_price")
        header_parts.append(f"{coin}_fluctate_rate_24H")
        header_parts.append(f"{coin}_units_traded_24H")

    if not os.path.exists(file_path) or os.stat(file_path).st_size == 0:
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(",".join(header_parts) + "\n")
            print_and_log(f"새로운 CSV 파일 '{file_path}'이 생성되고 헤더가 작성되었습니다.")
        except IOError as e:
            print_and_log(f"CSV 파일 헤더 작성 중 오류 발생: {e}")
    else:
        print_and_log(f"기존 CSV 파일 '{file_path}'에 데이터를 추가합니다.")

# 이상 징후 분석 및 알림 함수 (24시간 변동률, 추세 분석)
def analyze_and_notify_anomaly(df, fluctuation_threshold, deviation_threshold, target_coins, current_timestamp, ma_window):
    anomalies_detected_this_cycle = False
    print_and_log(f"\n--- [{current_timestamp}] 현재가/추세 분석 결과 ---")

    if len(df) < ma_window + 1: 
        print_and_log(f"  데이터 부족: 이동 평균 계산을 위해 최소 {ma_window + 1}개의 데이터가 필요합니다. 현재 {len(df)}개.")
        print_and_log("  추세 분석을 건너뛰고 24시간 변동률만 확인합니다.")
        
        for coin in target_coins:
            fluctuation_col_name = f"{coin}_fluctate_rate_24H"
            fluctuate_rate_str = df.iloc[-1].get(fluctuation_col_name)

            processed_fluctuate_str = str(fluctuate_rate_str).strip().upper()

            if pd.isna(fluctuate_rate_str) or processed_fluctuate_str == 'N/A' or processed_fluctuate_str == '':
                print_and_log(f"  {coin}: 변동률 데이터 없음 (N/A 또는 비어있음)")
                continue
            
            try:
                fluctuate_rate = float(fluctuate_rate_str)
                if abs(fluctuate_rate) >= fluctuation_threshold:
                    print_and_log(f"  [!!! 24H 변동률 이상 감지 !!!] {coin}: 24시간 변동률 = {fluctuate_rate:.2f}% (임계치 {fluctuation_threshold}%)", is_anomaly=True)
                    anomalies_detected_this_cycle = True
            except ValueError:
                print_and_log(f"  {coin}: 변동률 데이터 '{fluctuate_rate_str}'를 숫자로 변환할 수 없습니다. (데이터 형식 오류)")
        
        if not anomalies_detected_this_cycle:
            print_and_log("  => 현재 시점에서 특별한 이상 징후는 감지되지 않았습니다.")
        print_and_log("------------------------------------")
        return

    latest_data = df.iloc[-1]

    for coin in target_coins:
        closing_price_col = f"{coin}_closing_price"
        fluctuation_col_name = f"{coin}_fluctate_rate_24H"
        
        current_closing_price = latest_data.get(closing_price_col)
        
        if pd.isna(current_closing_price) or current_closing_price == 'N/A' or str(current_closing_price).strip() == '':
            print_and_log(f"  {coin}: 현재가 데이터 없음 (N/A 또는 비어있음). 추세 및 변동률 분석 건너뜀.")
            continue

        try:
            current_closing_price_float = float(current_closing_price)
            
            fluctuate_rate_str = latest_data.get(fluctuation_col_name) 
            processed_fluctuate_str = str(fluctuate_rate_str).strip().upper()

            if not (pd.isna(fluctuate_rate_str) or processed_fluctuate_str == 'N/A' or processed_fluctuate_str == ''):
                fluctuate_rate = float(fluctuate_rate_str)
                if abs(fluctuate_rate) >= fluctuation_threshold:
                    print_and_log(f"  [!!! 24H 변동률 이상 감지 !!!] {coin}: 24시간 변동률 = {fluctuate_rate:.2f}% (임계치 {fluctuation_threshold}%)", is_anomaly=True)
                    anomalies_detected_this_cycle = True
            else:
                print_and_log(f"  {coin}: 24시간 변동률 데이터 없음.")

            recent_prices = df[closing_price_col].tail(ma_window)
            recent_prices_numeric = pd.to_numeric(recent_prices, errors='coerce').dropna()
            
            if len(recent_prices_numeric) > 0:
                moving_average = recent_prices_numeric.mean()
                
                if moving_average != 0:
                    deviation_from_ma = ((current_closing_price_float - moving_average) / moving_average) * 100

                    if abs(deviation_from_ma) >= deviation_threshold:
                        print_and_log(f"  🚨🚨🚨 [추세 이탈 이상 감지!] 🚨🚨🚨")
                        print_and_log(f"  코인: {coin}")
                        print_and_log(f"  현재가: {current_closing_price_float:.2f} KRW")
                        print_and_log(f"  이동 평균({ma_window}개): {moving_average:.2f} KRW")
                        print_and_log(f"  추세 이탈률: {deviation_from_ma:.2f}% (임계치 ±{deviation_threshold}%)")
                        print_and_log(f"  경고: {coin}의 현재가가 이동 평균선에서 크게 벗어났습니다!", is_anomaly=True)
                        print_and_log(f"  🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨")
                        anomalies_detected_this_cycle = True
                else:
                    print_and_log(f"  {coin}: 이동 평균 계산을 위한 데이터의 평균이 0입니다. (이상)")
            else:
                print_and_log(f"  {coin}: 이동 평균 계산을 위한 유효한 최근 가격 데이터가 충분하지 않습니다.")

        except ValueError:
            print_and_log(f"  {coin}: 가격 데이터 '{current_closing_price}'를 숫자로 변환할 수 없습니다. (데이터 형식 오류)")
            
    if not anomalies_detected_this_cycle:
        print_and_log("\n  => 현재 시점에서 특별한 이상 징후는 감지되지 않았습니다.")
    
    print_and_log("------------------------------------")

# 자전거래 탐지 함수 (Public API 기반)
def detect_wash_trading(coin_symbol, time_window_seconds, price_tolerance_percent, quantity_tolerance_percent, lookback_count, current_timestamp):
    wash_trade_detected = False
    
    TRADE_HISTORY_ENDPOINT = f"/transaction_history/{coin_symbol}_KRW"
    TRADE_HISTORY_URL = BASE_URL + TRADE_HISTORY_ENDPOINT
    
    print_and_log(f"\n--- [{current_timestamp}] {coin_symbol} 자전거래 탐지 시도 ---")

    try:
        trade_response = requests.get(TRADE_HISTORY_URL, params={'count': lookback_count}, headers={})
        
        if trade_response.status_code == 200:
            trade_data = trade_response.json()
            
            if 'data' in trade_data and isinstance(trade_data['data'], list):
                trades_processed = []
                for trade in trade_data['data']:
                    try:
                        # 1. transaction_date 파싱 시도 (밀리초 포함, 없으면 밀리초 없이 재시도)
                        try:
                            trade_datetime = datetime.strptime(trade['transaction_date'], '%Y-%m-%d %H:%M:%S.%f')
                        except ValueError:
                            trade_datetime = datetime.strptime(trade['transaction_date'], '%Y-%m-%d %H:%M:%S')

                        units_traded_float = float(trade['units_traded'])
                        price_float = float(trade['price'])
                        
                        trades_processed.append({
                            'datetime_obj': trade_datetime,
                            'type': trade['type'],
                            'units_traded_float': units_traded_float,
                            'price_float': price_float,
                            'transaction_date': trade['transaction_date']
                        })
                    except (ValueError, KeyError) as e:
                        print_and_log(f"  {coin_symbol} 체결 내역 데이터 파싱 오류 (자전거래): {e} (데이터: {trade})")
                        continue
                
                trades_processed.sort(key=lambda x: x['datetime_obj'])

                for i in range(len(trades_processed)):
                    trade1 = trades_processed[i]
                    for j in range(i + 1, len(trades_processed)):
                        trade2 = trades_processed[j]

                        time_diff = (trade2['datetime_obj'] - trade1['datetime_obj']).total_seconds()
                        if time_diff > time_window_seconds:
                            break 
                        
                        if (trade1['type'] == 'bid' and trade2['type'] == 'ask') or \
                           (trade1['type'] == 'ask' and trade2['type'] == 'bid'):
                            
                            if trade1['price_float'] == 0: continue 
                            price_diff_percent = abs((trade1['price_float'] - trade2['price_float']) / trade1['price_float']) * 100
                            
                            if price_diff_percent <= price_tolerance_percent:
                                if trade1['units_traded_float'] == 0: continue
                                quantity_diff_percent = abs((trade1['units_traded_float'] - trade2['units_traded_float']) / trade1['units_traded_float']) * 100
                                if quantity_diff_percent <= quantity_tolerance_percent:
                                    
                                    print_and_log(f"  ⚡⚡⚡ [자전거래 의심 감지!] ⚡⚡⚡")
                                    print_and_log(f"  코인: {coin_symbol}")
                                    print_and_log(f"  거래 1 ({trade1['type']}): 시각={trade1['transaction_date']}, 가격={trade1['price_float']:.4f}, 수량={trade1['units_traded_float']:.4f}")
                                    print_and_log(f"  거래 2 ({trade2['type']}): 시각={trade2['transaction_date']}, 가격={trade2['price_float']:.4f}, 수량={trade2['units_traded_float']:.4f}")
                                    print_and_log(f"  시간 차이: {time_diff:.2f}초 (임계치 {time_window_seconds}초)")
                                    print_and_log(f"  가격 차이: {price_diff_percent:.2f}% (임계치 {price_tolerance_percent}%)")
                                    print_and_log(f"  수량 차이: {quantity_diff_percent:.2f}% (임계치 {quantity_tolerance_percent}%)")
                                    print_and_log(f"  경고: {coin_symbol} 마켓에서 짧은 시간 내 유사 가격/수량의 매수/매도 패턴이 감지되었습니다!", is_anomaly=True)
                                    print_and_log(f"  ⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡")
                                    wash_trade_detected = True

            else:
                print_and_log(f"  {coin_symbol}: 체결 내역 데이터가 없거나 리스트 형태가 아닙니다.")

        else:
            print_and_log(f"  {coin_symbol}: 체결 내역 API 요청 실패. 상태 코드: {trade_response.status_code}")
            print_and_log(f"  에러 메시지: {trade_response.text}")

    except requests.exceptions.RequestException as e:
        print_and_log(f"  [오류] {coin_symbol} 체결 내역 API 요청 중 네트워크 예외 발생: {e}")
    except Exception as e:
        print_and_log(f"  [오류] {coin_symbol} 자전거래 탐지 중 예상치 못한 오류: {e}")
        print_and_log(f"  상세: {traceback.format_exc()}")
        
    if not wash_trade_detected:
        print_and_log(f"  {coin_symbol}: 자전거래 의심 패턴 감지되지 않음.")
    print_and_log("------------------------------------")
    return wash_trade_detected

# 호가창 매물벽 탐지 함수 (스푸핑 탐지를 위해 감지된 벽 리스트도 반환)
def detect_order_book_wall_anomaly(coin_symbol, current_closing_price, current_units_traded_24H, price_distance_percent, volume_multiplier, lookback_count, current_timestamp):
    order_book_anomaly_detected = False
    detected_walls_this_cycle = [] # 현재 주기에서 감지된 대규모 벽 목록
    
    ORDER_BOOK_ENDPOINT = f"/orderbook/{coin_symbol}_KRW"
    ORDER_BOOK_URL = BASE_URL + ORDER_BOOK_ENDPOINT
    
    print_and_log(f"\n--- [{current_timestamp}] {coin_symbol} 호가창 매물벽 탐지 시도 ---")

    if current_closing_price == 'N/A' or current_units_traded_24H == 'N/A':
        print_and_log(f"  {coin_symbol}: 현재가 또는 24시간 거래량 데이터 부족. 매물벽 탐지 건너뜀.")
        print_and_log("------------------------------------")
        return False, [] # 빈 리스트 반환

    try:
        current_closing_price_float = float(current_closing_price)
        current_units_traded_24H_float = float(current_units_traded_24H)
        
        min_base_volume_for_wall = 100 
        threshold_volume_for_wall = max(min_base_volume_for_wall, current_units_traded_24H_float * 0.001) * volume_multiplier

        order_book_response = requests.get(ORDER_BOOK_URL, params={'count': lookback_count}, headers={})
        
        if order_book_response.status_code == 200:
            order_book_data = order_book_response.json()
            
            if 'data' in order_book_data:
                # 매도 호가 (asks) 분석
                for ask in order_book_data['data']['asks']:
                    try:
                        ask_price = float(ask['price'])
                        ask_qty = float(ask['quantity'])
                        
                        if current_closing_price_float == 0: continue 
                        price_deviation = abs((ask_price - current_closing_price_float) / current_closing_price_float) * 100
                        
                        if price_deviation >= price_distance_percent and ask_qty >= threshold_volume_for_wall:
                            print_and_log(f"  🧱⬆️ [호가창 매물벽 감지 - 매도벽!] 🧱⬆️")
                            print_and_log(f"  코인: {coin_symbol}")
                            print_and_log(f"  종류: 매도 (Ask)")
                            print_and_log(f"  호가: {ask_price:.2f} KRW")
                            print_and_log(f"  수량: {ask_qty:.4f} {coin_symbol} (임계치 {threshold_volume_for_wall:.4f} {coin_symbol})")
                            print_and_log(f"  현재가 대비 이탈률: {price_deviation:.2f}% (임계치 {price_distance_percent}%)")
                            print_and_log(f"  경고: {coin_symbol} 매도 호가창에 비정상적인 대규모 매물벽이 감지되었습니다!", is_anomaly=True)
                            print_and_log(f"  🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱")
                            order_book_anomaly_detected = True
                            detected_walls_this_cycle.append({
                                'price': ask_price,
                                'quantity': ask_qty,
                                'type': 'ask',
                                'timestamp': current_timestamp # 벽이 감지된 시간
                            })
                    except (ValueError, KeyError) as e:
                        print_and_log(f"  {coin_symbol} 매도 호가 데이터 파싱 오류: {e} (데이터: {ask})")
                        continue

                # 매수 호가 (bids) 분석
                for bid in order_book_data['data']['bids']:
                    try:
                        bid_price = float(bid['price'])
                        bid_qty = float(bid['quantity'])
                        
                        if current_closing_price_float == 0: continue
                        price_deviation = abs((bid_price - current_closing_price_float) / current_closing_price_float) * 100
                        
                        if price_deviation >= price_distance_percent and bid_qty >= threshold_volume_for_wall:
                            print_and_log(f"  🧱⬇️ [호가창 매물벽 감지 - 매수벽!] 🧱⬇️")
                            print_and_log(f"  코인: {coin_symbol}")
                            print_and_log(f"  종류: 매수 (Bid)")
                            print_and_log(f"  호가: {bid_price:.2f} KRW")
                            print_and_log(f"  수량: {bid_qty:.4f} {coin_symbol} (임계치 {threshold_volume_for_wall:.4f} {coin_symbol})")
                            print_and_log(f"  현재가 대비 이탈률: {price_deviation:.2f}% (임계치 {price_distance_percent}%)")
                            print_and_log(f"  경고: {coin_symbol} 매수 호가창에 비정상적인 대규모 매물벽이 감지되었습니다!", is_anomaly=True)
                            print_and_log(f"  🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱🧱")
                            order_book_anomaly_detected = True
                            detected_walls_this_cycle.append({
                                'price': bid_price,
                                'quantity': bid_qty,
                                'type': 'bid',
                                'timestamp': current_timestamp # 벽이 감지된 시간
                            })
                    except (ValueError, KeyError) as e:
                        print_and_log(f"  {coin_symbol} 매수 호가 데이터 파싱 오류: {e} (데이터: {bid})")
                        continue

            else: # <--- 이 부분이 예전 코드에서 723번 줄 근처에 해당할 수 있는 else 입니다.
                print_and_log(f"  {coin_symbol}: 호가창 데이터가 없거나 예상과 다릅니다.")

        else:
            print_and_log(f"  {coin_symbol}: 호가창 API 요청 실패. 상태 코드: {order_book_response.status_code}")
            print_and_log(f"  에러 메시지: {order_book_response.text}")

    except requests.exceptions.RequestException as e:
        print_and_log(f"  [오류] {coin_symbol} 호가창 API 요청 중 네트워크 예외 발생: {e}")
    except Exception as e:
        print_and_log(f"  [오류] {coin_symbol} 호가창 매물벽 탐지 중 예상치 못한 오류: {e}")
        
    if not order_book_anomaly_detected:
        print_and_log(f"  {coin_symbol}: 호가창 매물벽 패턴 감지되지 않음.")
    print_and_log("------------------------------------")
    return order_book_anomaly_detected, detected_walls_this_cycle # 감지된 벽 리스트 반환

# 펌프 앤 덤프 탐지 함수 (주로 펌프 단계에 집중)
def detect_pump_and_dump_anomaly(coin_symbol, df_full, pump_price_change_percent, pump_volume_multiplier_from_avg_24H, pump_detection_window_seconds, api_call_interval, current_timestamp):
    pump_detected = False
    
    print_and_log(f"\n--- [{current_timestamp}] {coin_symbol} 펌프 앤 덤프 탐지 시도 ---")

    num_points_for_pump_window = int(pump_detection_window_seconds / api_call_interval) + 1
    
    if len(df_full) < num_points_for_pump_window:
        print_and_log(f"  데이터 부족: 펌프 탐지를 위해 최소 {num_points_for_pump_window}개의 데이터가 필요합니다. 현재 {len(df_full)}개.")
        print_and_log("  펌프 앤 덤프 탐지 건너뜀.")
        print_and_log("------------------------------------")
        return False

    recent_prices_df = df_full.tail(num_points_for_pump_window)
    
    latest_df_timestamp = recent_prices_df['Timestamp (KST)'].iloc[-1]
    
    first_price_str = recent_prices_df[f"{coin_symbol}_closing_price"].iloc[0]
    last_price_str = recent_prices_df[f"{coin_symbol}_closing_price"].iloc[-1]
    
    if first_price_str == 'N/A' or last_price_str == 'N/A' or pd.isna(first_price_str) or pd.isna(last_price_str):
        print_and_log(f"  {coin_symbol}: 펌프 탐지를 위한 가격 데이터 부족 (N/A).")
        print_and_log("------------------------------------")
        return False

    try:
        first_price = float(first_price_str)
        last_price = float(last_price_str)
        
        if first_price == 0:
            print_and_log(f"  {coin_symbol}: 시작 가격이 0입니다. 가격 상승률 계산 불가.")
            print_and_log("------------------------------------")
            return False

        price_increase_percent = ((last_price - first_price) / first_price) * 100

        if price_increase_percent < pump_price_change_percent:
            print_and_log(f"  {coin_symbol}: 가격 상승률 ({price_increase_percent:.2f}%)이 임계치({pump_price_change_percent}%) 미만입니다.")
            print_and_log("------------------------------------")
            return False

    except ValueError as e:
        print_and_log(f"  {coin_symbol}: 펌프 탐지 가격 데이터 변환 오류: {e}")
        print_and_log("------------------------------------")
        return False

    TRADE_HISTORY_ENDPOINT = f"/transaction_history/{coin_symbol}_KRW"
    TRADE_HISTORY_URL = BASE_URL + TRADE_HISTORY_ENDPOINT

    try:
        trade_response = requests.get(TRADE_HISTORY_URL, params={'count': RECENT_TRADES_LOOKBACK_COUNT}, headers={})
        
        if trade_response.status_code == 200:
            trade_data = trade_response.json()
            
            if 'data' in trade_data and isinstance(trade_data['data'], list):
                trades_raw = trade_data['data']
                
                recent_window_volume = 0.0
                time_cutoff = datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S') - timedelta(seconds=pump_detection_window_seconds)

                for trade in trades_raw:
                    try:
                        # 1. transaction_date 파싱 시도 (밀리초 포함, 없으면 밀리초 없이 재시도)
                        try:
                            trade_datetime = datetime.strptime(trade['transaction_date'], '%Y-%m-%d %H:%M:%S.%f')
                        except ValueError:
                            trade_datetime = datetime.strptime(trade['transaction_date'], '%Y-%m-%d %H:%M:%S')

                        if trade_datetime >= time_cutoff:
                            recent_window_volume += float(trade['units_traded'])
                    except (ValueError, KeyError) as e:
                        print_and_log(f"  {coin_symbol} 체결 내역 데이터 파싱 오류 (펌프탐지): {e} (데이터: {trade})")
                        continue
                
                current_units_traded_24H = df_full[f"{coin_symbol}_units_traded_24H"].iloc[-1]
                if current_units_traded_24H == 'N/A' or pd.isna(current_units_traded_24H) or float(current_units_traded_24H) == 0:
                    print_and_log(f"  {coin_symbol}: 24시간 거래량 데이터 부족 또는 0. 펌프 탐지 불가.")
                    print_and_log("------------------------------------")
                    return False

                avg_volume_per_second_24H = float(current_units_traded_24H) / (24 * 3600)
                expected_volume_in_window = avg_volume_per_second_24H * pump_detection_window_seconds
                pump_volume_threshold = expected_volume_in_window * pump_volume_multiplier_from_avg_24H

                if recent_window_volume >= pump_volume_threshold:
                    print_and_log(f"  🚀📉 [펌프 앤 덤프 의심 감지!] 🚀📉")
                    print_and_log(f"  코인: {coin_symbol}")
                    print_and_log(f"  기간: 지난 {pump_detection_window_seconds}초")
                    print_and_log(f"  가격 상승률: {price_increase_percent:.2f}% (임계치 {pump_price_change_percent}%)")
                    print_and_log(f"  거래량: {recent_window_volume:.4f} {coin_symbol} (임계치 {pump_volume_threshold:.4f} {coin_symbol})")
                    print_and_log(f"  경고: {coin_symbol}에서 짧은 시간 내 가격 급등 및 비정상적 거래량 폭증 패턴이 감지되었습니다!", is_anomaly=True)
                    print_and_log(f"  🚀📉🚀📉🚀📉🚀📉🚀📉🚀📉🚀📉🚀📉🚀")
                    pump_detected = True
                else:
                    print_and_log(f"  {coin_symbol}: 거래량({recent_window_volume:.4f})이 펌프 임계치({pump_volume_threshold:.4f}) 미만입니다.")
                    print_and_log("------------------------------------")

            else:
                print_and_log(f"  {coin_symbol}: 체결 내역 데이터가 없거나 리스트 형태가 아닙니다. (펌프 탐지)")
                print_and_log("------------------------------------")

        else:
            print_and_log(f"  {coin_symbol}: 체결 내역 API 요청 실패. 상태 코드: {trade_response.status_code} (펌프 탐지)")
            print_and_log(f"  에러 메시지: {trade_response.text}")
            print_and_log("------------------------------------")

    except requests.exceptions.RequestException as e:
        print_and_log(f"  [오류] {coin_symbol} 체결 내역 API 요청 중 네트워크 예외 발생 (펌프탐지): {e}")
        print_and_log("------------------------------------")
    except Exception as e:
        print_and_log(f"  [오류] {coin_symbol} 펌프 앤 덤프 탐지 중 예상치 못한 오류: {e}")
        print_and_log("------------------------------------")
        
    if not pump_detected:
        print_and_log(f"  {coin_symbol}: 펌프 앤 덤프 의심 패턴 감지되지 않음.")
    print_and_log("------------------------------------")
    return pump_detected


# --- 메인 데이터 수집 및 분석 루프 ---
def data_collection_loop():
    global global_last_detected_large_walls # 전역 변수임을 명시
    
    write_csv_header_if_not_exists(CSV_FILE_PATH, TARGET_COINS)

    print_and_log("--- 이상거래 탐지 시스템 가동 시작 (데이터 수집 및 분석 전용 + 스푸핑 탐지) ---")
    print_and_log(f"대상 코인: {', '.join(TARGET_COINS)}")
    print_and_log(f"감시 주기: {API_CALL_INTERVAL_SECONDS}초")
    print_and_log(f"공유 데이터 파일: {os.path.abspath(SHARED_DATA_FILE)}")
    print_and_log("-" * 50)

    while True:
        current_time_kst = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        print_and_log(f"\n--- [{current_time_kst}] 데이터 수집 및 분석 시도 ---")
        
        current_prices_and_volumes = {} # 호가창 및 펌프 탐지에 필요한 현재가와 24H 거래량 저장을 위함
        current_detected_large_walls_this_cycle = {coin: [] for coin in TARGET_COINS} # 현재 주기에서 감지된 큰 벽들
        
        try: # <<<<<<<<<< 이 'try' 블록이 이번 루프 전체를 감싸고 있습니다.
            ticker_response = requests.get(TICKER_REQUEST_URL, headers={})

            if ticker_response.status_code == 200:
                ticker_data = ticker_response.json()

                if 'data' in ticker_data and isinstance(ticker_data['data'], dict):
                    # 이더리움 데이터의 원본 API 응답 확인을 위한 로깅 (이전 요청으로 인해 추가됨)
                    if 'ETH' in ticker_data['data']:
                        # print_and_log(f"  [디버그] ETH 원본 API 데이터: {json.dumps(ticker_data['data']['ETH'], indent=2)}") # 너무 길면 주석 처리
                        pass
                    else:
                        print_and_log(f"  [디버그] ETH가 티커 응답 'data'에 없습니다.")

                    row_data = [current_time_kst]
                    all_target_coins_found = True

                    for ticker_symbol in TARGET_COINS:
                        coin_info = ticker_data['data'].get(ticker_symbol)
                        
                        current_data_point = {
                            "timestamp": current_time_kst,
                            "closing_price": "N/A",
                            "fluctate_rate_24H": "N/A",
                            "units_traded_24H": "N/A"
                        }

                        if coin_info and coin_info is not None: # 'None' 값도 필터링
                            closing_price = coin_info.get('closing_price', 'N/A')
                            fluctate_rate_24H = coin_info.get('fluctate_rate_24H', 'N/A')
                            units_traded_24H = coin_info.get('units_traded_24H', 'N/A')
                            
                            row_data.extend([closing_price, fluctate_rate_24H, units_traded_24H])
                            current_prices_and_volumes[ticker_symbol] = {
                                'closing_price': closing_price,
                                'units_traded_24H': units_traded_24H
                            }
                            # 전역 데이터 이력에 추가 및 최신 요약 업데이트
                            current_data_point["closing_price"] = closing_price
                            current_data_point["fluctate_rate_24H"] = fluctate_rate_24H
                            current_data_point["units_traded_24H"] = units_traded_24H
                            global_current_summary[ticker_symbol] = current_data_point # 최신 요약 업데이트
                        else:
                            row_data.extend(['N/A', 'N/A', 'N/A'])
                            all_target_coins_found = False
                            current_prices_and_volumes[ticker_symbol] = {
                                'closing_price': 'N/A',
                                'units_traded_24H': 'N/A'
                            }
                            global_current_summary[ticker_symbol] = current_data_point # 최신 요약 업데이트 (N/A 값 포함)

                        # 전역 코인 데이터 이력에 현재 데이터 포인트 추가
                        global_coin_data_history[ticker_symbol].append(current_data_point)
                        # 이력 길이 유지
                        if len(global_coin_data_history[ticker_symbol]) > DATA_HISTORY_LENGTH:
                            global_coin_data_history[ticker_symbol].pop(0)

                    # 1.1. CSV 파일에 현재가 데이터 저장
                    try:
                        with open(CSV_FILE_PATH, 'a', encoding='utf-8') as f:
                            f.write(",".join(map(str, row_data)) + "\n")
                        print_and_log(f"  현재가 데이터 CSV에 저장 완료.")
                        if not all_target_coins_found:
                            print_and_log("  (경고: 일부 대상 코인 현재가 데이터가 이번 응답에 없었습니다. 로그 확인 요망)")

                    except IOError as e:
                        print_and_log(f"  [오류] CSV 파일 쓰기 오류 발생: {e}")
                    
                    # 2. 저장된 데이터 불러와서 24H 변동률 및 추세 분석
                    df_full = pd.DataFrame() # 빈 DataFrame으로 초기화
                    try:
                        df_full = pd.read_csv(CSV_FILE_PATH, parse_dates=["Timestamp (KST)"])
                        
                        if not df_full.empty:
                            analyze_and_notify_anomaly(df_full, FLUCTUATION_THRESHOLD_PERCENT, DEVIATION_THRESHOLD_PERCENT, TARGET_COINS, current_time_kst, MOVING_AVERAGE_WINDOW)
                        else:
                            print_and_log(f"  CSV 파일 '{CSV_FILE_PATH}'이 비어 있어 현재가/추세 분석을 건너뜠습니다.")

                    except FileNotFoundError:
                        print_and_log(f"  [오류] CSV 파일 '{CSV_FILE_PATH}'을(를) 찾을 수 없어 현재가/추세 분석을 건너킵니다.")
                    except pd.errors.EmptyDataError:
                        print_and_log(f"  [오류] CSV 파일 '{CSV_FILE_PATH}'이 비어 있어 현재가/추세 분석을 건너뜜.")
                    except Exception as e:
                        print_and_log(f"  [오류] CSV 데이터 읽기 또는 현재가/추세 분석 중 예상치 못한 오류: {e}")
                        print_and_log(f"  상세: {traceback.format_exc()}")
                    
                    # 3. 각 코인별 자전거래 탐지 실행
                    for coin_symbol in TARGET_COINS:
                        detect_wash_trading(coin_symbol, WASH_TRADE_TIME_WINDOW_SECONDS, WASH_TRADE_PRICE_TOLERANCE_PERCENT, WASH_TRADE_QUANTITY_TOLERANCE_PERCENT, RECENT_TRADES_LOOKBACK_COUNT, current_time_kst)

                    # --- 호가창 매물벽 탐지 및 스푸핑 감지를 위한 데이터 수집 ---
                    current_run_detected_walls = {coin: [] for coin in TARGET_COINS} # 이번 주기에서 탐지된 벽들을 임시 저장
                    for coin_symbol in TARGET_COINS:
                        coin_data = current_prices_and_volumes.get(coin_symbol, {'closing_price': 'N/A', 'units_traded_24H': 'N/A'})
                        
                        # detect_order_book_wall_anomaly 함수가 이제 두 번째 반환값(detected_walls_for_coin)을 추가로 줍니다.
                        is_wall_anomaly, detected_walls_for_coin = detect_order_book_wall_anomaly(
                            coin_symbol, 
                            coin_data['closing_price'], 
                            coin_data['units_traded_24H'], 
                            ORDER_WALL_PRICE_DISTANCE_PERCENT, 
                            ORDER_WALL_VOLUME_MULTIPLIER, 
                            ORDER_BOOK_COUNT,
                            current_time_kst
                        )
                        current_run_detected_walls[coin_symbol] = detected_walls_for_coin

                        # --- 스푸핑 탐지 로직 (여기서 실제 비교 및 알림) ---
                        # 직전 주기에서 감지된 벽들과 현재 주기에서 감지된 벽들을 비교하여 사라진 벽을 찾습니다.
                        for last_wall in global_last_detected_large_walls[coin_symbol]:
                            is_still_present = False
                            # 현재 주기에서 last_wall과 유사한 벽이 있는지 확인
                            for current_wall in detected_walls_for_coin:
                                # 가격 및 수량에 작은 오차를 허용하여 동일한 벽으로 간주
                                if (last_wall['type'] == current_wall['type'] and
                                    # 가격 차이가 0.01% 이내
                                    abs((last_wall['price'] - current_wall['price']) / last_wall['price']) * 100 < 0.01 and 
                                    # 수량 차이가 0.01% 이내
                                    abs((last_wall['quantity'] - current_wall['quantity']) / last_wall['quantity']) * 100 < 0.01): 
                                    is_still_present = True
                                    break
                            
                            if not is_still_present:
                                # 벽이 사라졌고, 가격이 그 벽을 향해 유의미하게 움직이지 않았는지 확인
                                # 즉, 매도벽이 사라졌는데 가격이 올라가지 않았거나, 매수벽이 사라졌는데 가격이 내려가지 않았다면
                                current_closing_price_float = float(current_prices_and_volumes[coin_symbol]['closing_price'])
                                spoofing_condition_met = False

                                if last_wall['type'] == 'ask': # 매도벽 (위에서 눌러주는 역할)
                                    # 매도벽이 사라졌는데 현재가가 그 벽 가격보다 "현저히 아래에" 머물러 있다면 의심
                                    # (가격이 그 벽을 뚫지 못하고 벽이 사라진 경우)
                                    # 즉, 벽이 사라졌음에도 불구하고 가격이 그 벽을 넘어 올라가지 못하고 한참 아래에 있다면 스푸핑 의심
                                    if current_closing_price_float < last_wall['price'] * (1 - ORDER_WALL_PRICE_DISTANCE_PERCENT / 100 * 0.2): # 원래 벽 거리의 20% 이상 벗어나지 않았다면
                                        spoofing_condition_met = True
                                elif last_wall['type'] == 'bid': # 매수벽 (아래에서 받쳐주는 역할)
                                    # 매수벽이 사라졌는데 현재가가 그 벽 가격보다 "현저히 위에" 머물러 있다면 의심
                                    # (가격이 그 벽 아래로 내려가지 못하고 벽이 사라진 경우)
                                    # 즉, 벽이 사라졌음에도 불구하고 가격이 그 벽을 넘어 내려가지 못하고 한참 위에 있다면 스푸핑 의심
                                    if current_closing_price_float > last_wall['price'] * (1 + ORDER_WALL_PRICE_DISTANCE_PERCENT / 100 * 0.2): # 원래 벽 거리의 20% 이상 벗어나지 않았다면
                                        spoofing_condition_met = True
                                
                                if spoofing_condition_met:
                                    print_and_log(f"  👻 [스푸핑 의심 감지!] 👻")
                                    print_and_log(f"  코인: {coin_symbol}")
                                    print_and_log(f"  사라진 {last_wall['type']} 벽: 가격={last_wall['price']:.2f}, 수량={last_wall['quantity']:.4f}")
                                    print_and_log(f"  벽 감지 시각: {last_wall['timestamp']}")
                                    print_and_log(f"  현재가: {current_closing_price_float:.2f} KRW")
                                    print_and_log(f"  경고: {coin_symbol} 호가창에 나타났던 대규모 벽이 가격 이동 없이 사라졌습니다! (스푸핑 의심)", is_anomaly=True)
                                    print_and_log(f"  👻👻👻👻👻👻👻👻👻👻👻👻👻👻👻👻👻👻👻👻👻")
                        
                    # 다음 반복을 위해 현재 주기에서 감지된 벽을 '직전 주기 벽'으로 저장
                    global_last_detected_large_walls = current_run_detected_walls.copy()
                
                # 5. 각 코인별 펌프 앤 덤프 탐지 실행 (새로 추가)
                if not df_full.empty: # CSV 데이터가 있어야만 펌프 탐지 가능
                    for coin_symbol in TARGET_COINS:
                        detect_pump_and_dump_anomaly(
                            coin_symbol, 
                            df_full, 
                            PUMP_PRICE_INCREASE_PERCENT, 
                            PUMP_VOLUME_MULTIPLIER_FROM_AVG_24H, 
                            PUMP_DETECTION_WINDOW_SECONDS, 
                            API_CALL_INTERVAL_SECONDS, 
                            current_time_kst
                        )
                else:
                    print_and_log(f"  CSV 파일에 데이터가 없어 펌프 앤 덤프 탐지를 건너킵니다.")

            else: # <<<<<<<<<< 이 'else'가 문제가 되는 723번 줄일 가능성이 있습니다 (만약 들여쓰기가 이상해졌다면).
                # 이 'else'는 'if ticker_response.status_code == 200:' 에 대한 'else'가 아니라,
                # 'if 'data' in ticker_data and isinstance(ticker_data['data'], dict):' 에 대한 'else' 입니다.
                print_and_log(f"  티커 응답의 'data' 필드가 예상과 다릅니다. 원본 응답 구조 확인 필요.")

            # --- 중요: 전역 데이터를 공유 JSON 파일에 저장 ---
            try: # <<<<<<<<<< 이 'try' 블록은 바로 위에 있는 큰 'if' 블록 내부에 있습니다.
                with open(SHARED_DATA_FILE, 'w', encoding='utf-8') as f:
                    json.dump({
                        'history': global_coin_data_history,
                        'anomaly_logs': global_anomaly_logs,
                        'current_summary': global_current_summary,
                        'last_detected_large_walls': global_last_detected_large_walls # 스푸핑 데이터도 저장
                    }, f, ensure_ascii=False, indent=2)
                print_and_log(f"  공유 데이터 '{SHARED_DATA_FILE}'에 저장 완료.")
            except Exception as e:
                print_and_log(f"  [오류] 공유 데이터 '{SHARED_DATA_FILE}' 저장 실패: {e}")

        except requests.exceptions.RequestException as e: # <<<<<<<<<< 이 'except'는 가장 바깥쪽 'try'의 예외 처리입니다.
            print_and_log(f"  [오류] API 요청 중 네트워크 예외 발생: {e}")
            print_and_log("  네트워크 연결 상태를 확인하거나 잠시 후 다시 시도합니다.")
        except Exception as e: # <<<<<<<<<< 이 'except'도 가장 바깥쪽 'try'의 예외 처리입니다.
            print_and_log(f"  [치명적 오류] 예상치 못한 오류 발생. 프로그램 종료: {e}")
            print_and_log(f"  상세: {traceback.format_exc()}")
            break # 이 break는 while True 루프를 빠져나오게 합니다.

    # 이 time.sleep은 while True 루프의 마지막에, 가장 바깥쪽 'try-except' 블록의 외부(동일한 들여쓰기 레벨)에 위치해야 합니다.
    time.sleep(API_CALL_INTERVAL_SECONDS)

# --- 메인 실행 흐름 ---
if __name__ == "__main__":
    # main 함수가 없으므로 직접 data_collection_loop를 호출
    data_collection_loop()
