# web_server.py
# 파트너, 이 스크립트는 Flask 웹 서버를 실행하여 웹 UI와 데이터를 서비스합니다.
# 'anomaly_detection_system.py'에서 생성된 'shared_data.json' 파일을 읽어와 사용합니다.

import subprocess
import sys
import os
import json
import time

# Flask 라이브러리 설치 확인 및 설치
try:
    from flask import Flask, jsonify, send_from_directory
except ImportError:
    print("Flask 라이브러리가 설치되어 있지 않습니다. 설치를 시도합니다...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "Flask"])
        from flask import Flask, jsonify, send_from_directory
        print("Flask 라이브러리 설치 완료.")
    except Exception as e:
        print(f"Flask 라이브러리 설치 실패: {e}")
        print("웹 서버를 시작할 수 없습니다. 프로그램을 종료합니다.")
        sys.exit(1) # Flask 설치 실패 시 프로그램 종료

# --- 설정 ---
WEB_SERVER_PORT = 5000 # 웹 서버가 실행될 포트
SHARED_DATA_FILE = "shared_data.json" # 데이터 수집 스크립트와 공유하는 파일명

app = Flask(__name__)

# 루트 경로 ('/')에 접속 시 'index.html' 파일 제공
@app.route('/')
def index():
    # 현재 디렉토리에서 'index.html' 파일을 찾아 제공합니다.
    # 이 파일은 UI를 담당합니다.
    return send_from_directory('.', 'index.html')

# 데이터 API 엔드포인트 ('/data')에 접속 시 'shared_data.json' 내용 제공
@app.route('/data')
def get_data():
    # shared_data.json 파일이 존재하는지 확인
    if not os.path.exists(SHARED_DATA_FILE):
        # 파일이 없으면 오류 메시지와 함께 404 상태 코드 반환
        return jsonify({
            "history": {}, 
            "anomaly_logs": [f"오류: 데이터 파일이 없습니다. anomaly_detection_system.py를 먼저 실행하세요."], 
            "current_summary": {}
        }), 404
    
    try:
        # shared_data.json 파일 읽기
        with open(SHARED_DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # 읽어온 JSON 데이터를 클라이언트에 응답으로 반환
        return jsonify(data)
    except json.JSONDecodeError:
        # JSON 파싱 오류 발생 시 500 상태 코드와 오류 메시지 반환
        return jsonify({
            "history": {}, 
            "anomaly_logs": [f"오류: 데이터 파일 '{SHARED_DATA_FILE}'이 손상되었습니다. anomaly_detection_system.py를 다시 시작해주세요."], 
            "current_summary": {}
        }), 500
    except Exception as e:
        # 기타 예외 발생 시 500 상태 코드와 오류 메시지 반환
        return jsonify({
            "history": {}, 
            "anomaly_logs": [f"오류: 데이터 로드 중 예상치 못한 오류 발생: {e}"], 
            "current_summary": {}
        }), 500

if __name__ == '__main__':
    print(f"--- Flask 웹 서버 시작 (http://127.0.0.1:{WEB_SERVER_PORT}) ---")
    print(f"웹 UI를 보려면 브라우저에서 이 주소로 접속하세요.")
    # Flask 앱 실행. debug=False로 설정하여 운영 환경에 적합하게.
    # use_reloader=False는 스레딩과 함께 사용 시 문제가 발생할 수 있어 비활성화.
    app.run(host='127.0.0.1', port=WEB_SERVER_PORT, debug=False, use_reloader=False)
