import streamlit as st
import pandas as pd
import os
import time
import io
import subprocess
from datetime import datetime
import requests
import json

# 환경 감지 함수
def is_running_on_streamlit_cloud():
    """Streamlit Cloud에서 실행 중인지 확인"""
    # Streamlit Cloud에서 설정되는 환경변수들 확인
    cloud_indicators = [
        'STREAMLIT_SERVER_HEADLESS',
        'STREAMLIT_SERVER_PORT',
        'STREAMLIT_SERVER_ADDRESS',
        'STREAMLIT_CLOUD_ENVIRONMENT',
        'STREAMLIT_SERVER_RUN_ON_SAVE',
        'STREAMLIT_SERVER_FILE_WATCHER_TYPE'
    ]
    
    # 추가로 Streamlit Cloud의 특정 경로나 설정 확인
    cloud_path_indicators = [
        '/app',
        '/home/appuser',
        '/opt/streamlit'
    ]
    
    # 환경변수 확인
    env_check = any(os.environ.get(indicator) for indicator in cloud_indicators)
    
    # 경로 확인
    path_check = any(os.path.exists(path) for path in cloud_path_indicators)
    
    return env_check or path_check

# =============================================================================
# 파일 경로 설정
# =============================================================================

# 현재 스크립트의 디렉토리를 기준으로 상대 경로 설정
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

# 데이터 파일 경로
ASSIGNMENT_FILE = os.path.join(DATA_DIR, "assignment_history.csv")
EXECUTION_FILE = os.path.join(DATA_DIR, "execution_status.csv")
INFLUENCER_FILE = os.path.join(DATA_DIR, "influencer.csv")
MONTHLY_TARGETS_FILE = os.path.join(DATA_DIR, "monthly_assignment_targets.csv")

# 데이터 디렉토리가 없으면 생성
os.makedirs(DATA_DIR, exist_ok=True)

# =============================================================================
# GitHub Actions 자동 동기화 기능
# =============================================================================

def update_file_via_github_api(file_path, content, commit_message):
    """GitHub API를 사용해서 파일을 직접 업데이트"""
    try:
        # GitHub Personal Access Token (Streamlit Secrets에서 가져오기)
        github_token = st.secrets.get("GITHUB_TOKEN", "")
        repo_owner = st.secrets.get("GITHUB_REPO_OWNER", "jisu8")
        repo_name = st.secrets.get("GITHUB_REPO_NAME", "influencer-assignment")
        
        if not github_token:
            st.warning("⚠️ GitHub 토큰이 설정되지 않았습니다. 로컬에만 저장됩니다.")
            return False
        
        # GitHub API URL
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{file_path}"
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # 현재 파일의 SHA 가져오기 (파일이 존재하는 경우)
        response = requests.get(url, headers=headers)
        sha = None
        if response.status_code == 200:
            current_file = response.json()
            sha = current_file['sha']
        
        # 파일 업데이트
        import base64
        content_bytes = content.encode('utf-8')
        content_base64 = base64.b64encode(content_bytes).decode('utf-8')
        
        data = {
            "message": commit_message,
            "content": content_base64,
            "sha": sha
        }
        
        response = requests.put(url, headers=headers, json=data)
        
        if response.status_code in [200, 201]:
            return True
        else:
            st.error(f"❌ GitHub 업데이트 실패: {response.status_code}")
            if response.status_code == 401:
                st.error("인증 실패: GitHub 토큰을 확인해주세요.")
            elif response.status_code == 404:
                st.error("저장소를 찾을 수 없습니다: 저장소 이름을 확인해주세요.")
            elif response.status_code == 403:
                st.error("권한이 없습니다: 토큰 권한을 확인해주세요.")
            elif response.status_code == 422:
                st.error("파일 내용이 동일합니다.")
            return False
            
    except Exception as e:
        st.error(f"❌ GitHub 업데이트 중 오류: {e}")
        return False

def save_with_auto_sync(data, file_path, commit_message=None):
    """데이터 저장 후 GitHub API로 직접 업데이트 (클라우드에서만)"""
    try:
        # 로컬에 데이터 저장
        data.to_csv(file_path, index=False, encoding="utf-8")
        
        # 클라우드에서만 GitHub API 직접 업데이트 실행
        if is_running_on_streamlit_cloud():
            # 커밋 메시지 생성
            if commit_message is None:
                filename = os.path.basename(file_path)
                commit_message = f"Auto-update {filename}"
            
            # 파일 내용을 문자열로 변환
            content = data.to_csv(index=False, encoding="utf-8")
            
            # GitHub 저장소 내의 상대 경로로 변환
            relative_path = os.path.relpath(file_path, SCRIPT_DIR)
            relative_path = relative_path.replace('\\', '/')  # Windows 경로를 Unix 경로로 변환
            
            # GitHub API로 직접 업데이트 (알림 없이)
            sync_success = update_file_via_github_api(relative_path, content, commit_message)
            
            if not sync_success:
                st.warning("⚠️ GitHub 업데이트에 실패했습니다. 수동으로 데이터를 백업해주세요.")
        else:
            # 로컬에서는 동기화 없이 저장만
            st.info("💾 로컬에 저장되었습니다. (GitHub 동기화는 클라우드에서만 실행됩니다)")
        
        return True
        
    except Exception as e:
        st.error(f"❌ 데이터 저장 중 오류가 발생했습니다: {e}")
        return False

def save_local_only(data, file_path):
    """로컬에만 데이터 저장 (GitHub 동기화 없음)"""
    try:
        # 로컬에만 데이터 저장
        data.to_csv(file_path, index=False, encoding="utf-8")
        return True
        
    except Exception as e:
        st.error(f"❌ 로컬 데이터 저장 중 오류가 발생했습니다: {e}")
        return False

# =============================================================================
# 기존 파일 경로 설정 (로컬 백업용)
# =============================================================================

# SCRIPT_DIR은 이미 위에서 정의됨
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

# 데이터 파일 경로 (로컬 백업용)
ASSIGNMENT_FILE = os.path.join(DATA_DIR, "assignment_history.csv")
EXECUTION_FILE = os.path.join(DATA_DIR, "execution_status.csv")
INFLUENCER_FILE = os.path.join(DATA_DIR, "influencer.csv")

# 데이터 디렉토리가 없으면 생성
os.makedirs(DATA_DIR, exist_ok=True)

# =============================================================================
# GitHub 자동 푸시 기능
# =============================================================================

def auto_push_to_github(commit_message="Auto-update data files"):
    """데이터 변경 시 자동으로 GitHub에 푸시"""
    try:
        # 클라우드에서만 실행
        if not is_running_on_streamlit_cloud():
            print("Local environment detected. Skipping auto push to GitHub.")
            return False
            
        # Git 상태 확인
        result = subprocess.run(['git', 'status', '--porcelain'], 
                              capture_output=True, text=True, cwd=SCRIPT_DIR)
        
        if result.stdout.strip():  # 변경사항이 있는 경우
            # 변경사항 추가
            subprocess.run(['git', 'add', '.'], cwd=SCRIPT_DIR, check=True)
            
            # 커밋
            subprocess.run(['git', 'commit', '-m', commit_message], 
                         cwd=SCRIPT_DIR, check=True)
            
            # 원격 변경사항 먼저 가져오기 (충돌 방지)
            try:
                pull_result = subprocess.run(['git', 'pull', 'origin', 'master'], 
                                           cwd=SCRIPT_DIR, capture_output=True, text=True)
                if pull_result.returncode != 0:
                    print(f"Git pull warning: {pull_result.stderr}")
            except Exception as e:
                print(f"Git pull error: {e}")
            
            # 푸시 (더 강력한 에러 처리)
            push_result = subprocess.run(['git', 'push', 'origin', 'master'], 
                                       cwd=SCRIPT_DIR, capture_output=True, text=True)
            
            if push_result.returncode == 0:
                return True
            else:
                return False
        else:
            # 변경사항이 없는 경우
            return False
            
    except subprocess.CalledProcessError as e:
        return False
    except Exception as e:
        return False

def check_github_connection():
    """GitHub 연결 상태 확인"""
    try:
        # GitHub Personal Access Token 확인
        github_token = st.secrets.get("GITHUB_TOKEN", "")
        repo_owner = st.secrets.get("GITHUB_REPO_OWNER", "jisu8")
        repo_name = st.secrets.get("GITHUB_REPO_NAME", "influencer-assignment")
        
        if not github_token:
            st.sidebar.warning("⚠️ GitHub 토큰이 설정되지 않았습니다.")
            return False
        
        # GitHub API로 연결 테스트
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            st.sidebar.success("✅ GitHub 연결 성공!")
            repo_info = response.json()
            st.sidebar.info(f"📁 저장소: {repo_info['full_name']}")
            st.sidebar.info(f"🔗 URL: {repo_info['html_url']}")
            return True
        else:
            st.sidebar.error(f"❌ GitHub 연결 실패: {response.status_code}")
            return False
            
    except Exception as e:
        st.sidebar.error(f"❌ GitHub 연결 확인 중 오류: {e}")
        return False

def check_github_sync_status():
    """클라우드 → GitHub 동기화 상태 확인"""
    try:
        # GitHub Personal Access Token 확인
        github_token = st.secrets.get("GITHUB_TOKEN", "")
        repo_owner = st.secrets.get("GITHUB_REPO_OWNER", "jisu8")
        repo_name = st.secrets.get("GITHUB_REPO_NAME", "influencer-assignment")
        
        if not github_token:
            st.sidebar.warning("⚠️ GitHub 토큰이 설정되지 않았습니다.")
            return False
        
        # GitHub에서 최신 데이터 파일 확인
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # assignment_history.csv 파일 확인
        assignment_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/data/assignment_history.csv"
        assignment_response = requests.get(assignment_url, headers=headers)
        
        # execution_status.csv 파일 확인
        execution_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/data/execution_status.csv"
        execution_response = requests.get(execution_url, headers=headers)
        
        if assignment_response.status_code == 200 and execution_response.status_code == 200:
            assignment_data = assignment_response.json()
            execution_data = execution_response.json()
            
            # 파일의 마지막 수정 시간 확인 (안전하게 처리)
            try:
                assignment_updated = assignment_data.get('updated_at', '알 수 없음')
                execution_updated = execution_data.get('updated_at', '알 수 없음')
            except Exception as e:
                st.sidebar.error(f"❌ 파일 정보 파싱 오류: {e}")
                return False
            
            st.sidebar.success("✅ GitHub 동기화 상태 확인 완료!")
            st.sidebar.info(f"📊 배정 데이터: {assignment_updated}")
            st.sidebar.info(f"📈 집행 데이터: {execution_updated}")
            
            # 최근 5분 내에 업데이트되었는지 확인 (안전하게 처리)
            try:
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                
                if assignment_updated != '알 수 없음' and execution_updated != '알 수 없음':
                    assignment_time = datetime.fromisoformat(assignment_updated.replace('Z', '+00:00'))
                    execution_time = datetime.fromisoformat(execution_updated.replace('Z', '+00:00'))
                    
                    time_diff_assignment = (now - assignment_time).total_seconds() / 60
                    time_diff_execution = (now - execution_time).total_seconds() / 60
                    
                    if time_diff_assignment < 5 and time_diff_execution < 5:
                        st.sidebar.success("🟢 최근에 동기화됨 (5분 이내)")
                    else:
                        st.sidebar.warning("🟡 마지막 동기화가 오래됨 (5분 이상)")
                else:
                    st.sidebar.warning("⚠️ 파일 수정 시간을 확인할 수 없습니다.")
            except Exception as e:
                st.sidebar.warning(f"⚠️ 시간 비교 중 오류: {e}")
            
            return True
        else:
            st.sidebar.error("❌ GitHub에서 데이터 파일을 찾을 수 없습니다.")
            return False
            
    except Exception as e:
        st.sidebar.error(f"❌ GitHub 동기화 상태 확인 중 오류: {e}")
        return False

# =============================================================================
# 상수 정의
# =============================================================================

# 파일 경로
INFLUENCER_FILE = "data/influencer.csv"
ASSIGNMENT_FILE = "data/assignment_history.csv"
EXECUTION_FILE = "data/execution_status.csv"

# 브랜드 설정
BRANDS = ["MLB", "DX", "DV", "ST"]
BRAND_OPTIONS = ["전체"] + BRANDS

# 시즌 설정
SEASON_OPTIONS = ["25FW", "26SS", "26FW", "27SS"]
FW_MONTHS = ["9월", "10월", "11월", "12월", "1월", "2월"]
SS_MONTHS = ["3월", "4월", "5월", "6월", "7월", "8월"]

# 월별 이름 매핑
MONTH_NAMES = {
    9: '9월', 10: '10월', 11: '11월', 12: '12월',
    1: '1월', 2: '2월', 3: '3월', 4: '4월', 5: '5월', 6: '6월', 7: '7월', 8: '8월'
}

# 컬럼명 상수
COLUMN_NAMES = {
    'brand': '브랜드',
    'month': '배정월',
    'season': '시즌',
    'target_quantity': '배정요청수량',
    'assigned_quantity': '배정수량',
    'difference': '차이',
    'status': '상태',
    'name': '이름',
    'id': 'ID',
    'assignment_month': '배정월',
    'execution_status': '집행상태'
}

# 상태 옵션
STATUS_OPTIONS = ["📋 배정완료", "✅ 집행완료"]

# =============================================================================
# CSS 스타일
# =============================================================================

def load_css():
    """CSS 스타일 로드"""
    st.markdown("""
    <style>
        /* 전체 텍스트 크기 줄이기 (selectbox 제외) */
        .stMarkdown, .stText, .stNumberInput, .stButton, .stDataFrame {
            font-size: 0.9em !important;
        }
        
        /* selectbox는 기본 위치 유지 */
        .stSelectbox {
            font-size: 0.9em !important;
            position: relative !important;
            z-index: auto !important;
        }
        
        /* selectbox 드롭다운 위치 고정 */
        .stSelectbox > div > div {
            position: relative !important;
        }
        
        /* selectbox 옵션 리스트 위치 고정 */
        .stSelectbox ul, .stSelectbox li {
            position: relative !important;
            z-index: 1000 !important;
        }
        
        /* 헤더 크기 줄이기 */
        h1 { font-size: 1.8em !important; }
        h2 { font-size: 1.4em !important; }
        h3 { font-size: 1.2em !important; }
        
        /* 사이드바 전체 텍스트 크기 줄이기 */
        .css-1d391kg, .css-1lcbmhc, .css-1v0mbdj {
            font-size: 0.8em !important;
        }
        
        /* 사이드바 헤더 크기 줄이기 */
        .css-1d391kg h1, .css-1d391kg h2, .css-1d391kg h3 {
            font-size: 0.8em !important;
        }
        
        /* 사이드바 서브헤더 크기 줄이기 */
        .css-1d391kg .stSubheader {
            font-size: 0.7em !important;
        }
        
        /* 사이드바 라벨 크기 줄이기 */
        .css-1d391kg label {
            font-size: 0.65em !important;
        }
        
        /* 사이드바 입력 필드 크기 줄이기 */
        .css-1d391kg input, .css-1d391kg select {
            font-size: 0.65em !important;
        }
        
        /* 테이블 텍스트 크기 줄이기 */
        .stDataFrame {
            font-size: 0.8em !important;
        }
        
        /* 버튼 텍스트 크기 줄이기 */
        .stButton > button {
            font-size: 0.9em !important;
        }
        
        /* 탭 텍스트 크기 줄이기 */
        .stTabs [data-baseweb="tab-list"] {
            font-size: 0.9em !important;
        }
        
        /* 브랜드별 리스트 스타일 */
        .brand-list {
            margin: 8px 0;
            font-size: 0.85em;
        }
        .brand-title {
            color: #ff6b6b;
            font-weight: bold;
            margin-bottom: 6px;
            font-size: 0.9em;
            border-left: 3px solid #ff6b6b;
            padding-left: 8px;
        }
        .influencer-item {
            color: #ffffff;
            margin: 2px 0 2px 15px;
            font-size: 0.8em;
            padding: 2px 0;
        }
        

        
        /* 컨테이너 기반 일관된 레이아웃 - 임시 비활성화 */
        /*
        .stContainer {
            margin: 10px 0 !important;
            padding: 10px !important;
            border-radius: 5px !important;
        }
        
        .stContainer:first-child {
            min-height: 120px !important;
            max-height: 120px !important;
        }
        
        .stContainer:last-child {
            min-height: 600px !important;
        }
        
        .stDataFrame {
            min-width: 100% !important;
            width: 100% !important;
        }
        */
    </style>
    """, unsafe_allow_html=True)

# =============================================================================
# 유틸리티 함수들
# =============================================================================

def to_excel_bytes(df):
    """DataFrame을 Excel 바이트로 변환"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)
    return output.getvalue()

def create_multi_sheet_excel(influencer_summary, selected_brand_filter, selected_season_filter):
    """브랜드별 개별 시트가 포함된 Excel 파일 생성"""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # 1. 전체 시트
        influencer_summary.to_excel(writer, index=False, sheet_name='전체')
        
        # 2. 브랜드별 개별 시트 (상태값 표시)
        brands = ["MLB", "DX", "DV", "ST"]
        for brand in brands:
            # 해당 브랜드의 계약수가 있는 인플루언서만 필터링
            brand_contract_col = f"{brand}_계약수"
            if brand_contract_col in influencer_summary.columns:
                brand_data = influencer_summary[influencer_summary[brand_contract_col] > 0].copy()
                if not brand_data.empty:
                    # 브랜드별 시트에서는 해당 브랜드의 상태값을 표시하도록 월별 컬럼 수정
                    brand_data_with_status = apply_brand_status_to_monthly_columns(brand_data, brand)
                    brand_data_with_status.to_excel(writer, index=False, sheet_name=brand)
        
        # 3. 시즌별 시트
        if selected_season_filter:
            season_data = influencer_summary.copy()
            season_data.to_excel(writer, index=False, sheet_name=f"{selected_season_filter}")
    
    output.seek(0)
    return output.getvalue()

def apply_brand_status_to_monthly_columns(brand_data, target_brand):
    """브랜드별 시트에서 해당 브랜드의 상태값을 월별 컬럼에 적용"""
    months = ["9월", "10월", "11월", "12월", "1월", "2월"]
    brand_data_copy = brand_data.copy()
    
    # 월별 컬럼 초기화
    for month in months:
        brand_data_copy[month] = ""
    
    # 1. 집행완료된 배정 표시
    if os.path.exists(EXECUTION_FILE):
        execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
        if not execution_data.empty and '실제집행수' in execution_data.columns:
            # 해당 브랜드의 집행완료 데이터만 필터링
            completed_executions = execution_data[
                (execution_data['브랜드'] == target_brand) & 
                (execution_data['실제집행수'] > 0)
            ]
            
            # 인플루언서별, 월별로 상태 표시
            for _, row in brand_data_copy.iterrows():
                influencer_id = row["id"]
                for month in months:
                    # 해당 인플루언서의 해당 월 집행 내역
                    month_executions = completed_executions[
                        (completed_executions['id'] == influencer_id) & 
                        (completed_executions['배정월'] == month)
                    ]
                    
                    if not month_executions.empty:
                        brand_data_copy.loc[brand_data_copy['id'] == influencer_id, month] = "집행완료"
    
    # 2. 배정완료 상태인 배정 표시
    if os.path.exists(ASSIGNMENT_FILE):
        assignment_data = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        if not assignment_data.empty and '상태' in assignment_data.columns:
            # 해당 브랜드의 배정완료 데이터만 필터링
            completed_assignments = assignment_data[
                (assignment_data['브랜드'] == target_brand) & 
                (assignment_data['상태'] == '📋 배정완료')
            ]
            
            # 인플루언서별, 월별로 배정 상태 추가
            for _, row in brand_data_copy.iterrows():
                influencer_id = row["id"]
                for month in months:
                    # 해당 인플루언서의 해당 월 배정 내역
                    month_assignments = completed_assignments[
                        (completed_assignments['id'] == influencer_id) & 
                        (completed_assignments['배정월'] == month)
                    ]
                    
                    if not month_assignments.empty:
                        # 기존 집행완료 데이터가 있으면 추가, 없으면 새로 설정
                        current_value = brand_data_copy.loc[brand_data_copy['id'] == influencer_id, month].iloc[0]
                        if current_value and current_value.strip():
                            # 기존 값에 배정완료 상태 추가
                            if current_value == "집행완료":
                                brand_data_copy.loc[brand_data_copy['id'] == influencer_id, month] = "집행완료, 배정완료"
                            elif "집행완료" in current_value:
                                brand_data_copy.loc[brand_data_copy['id'] == influencer_id, month] = current_value + ", 배정완료"
                            else:
                                brand_data_copy.loc[brand_data_copy['id'] == influencer_id, month] = "배정완료"
                        else:
                            # 기존 값이 없으면 배정완료 상태만 표시
                            brand_data_copy.loc[brand_data_copy['id'] == influencer_id, month] = "배정완료"
    
    return brand_data_copy

def add_execution_data(df, execution_file=EXECUTION_FILE):
    """실행 데이터를 DataFrame에 추가하고 잔여수 계산"""
    if os.path.exists(execution_file):
        execution_data = pd.read_csv(execution_file, encoding="utf-8")
        if not execution_data.empty:
            # 브랜드_집행수 컬럼 초기화 (사용자가 직접 입력할 예정)
            if "브랜드_집행수" not in df.columns:
                df["브랜드_집행수"] = 0
        else:
            if "브랜드_집행수" not in df.columns:
                df["브랜드_집행수"] = 0
    else:
        if "브랜드_집행수" not in df.columns:
            df["브랜드_집행수"] = 0
    
    return df



def reorder_columns(df, expected_columns):
    """컬럼 순서 재정렬"""
    available_columns = [col for col in expected_columns if col in df.columns]
    if available_columns:
        return df[available_columns]
    return df

def get_month_options(season):
    """시즌에 따른 월 옵션 반환"""
    if season in ["25FW", "26FW"]:
        return FW_MONTHS
    elif season in ["26SS", "27SS"]:
        return SS_MONTHS
    else:
        return FW_MONTHS  # 기본값

def create_warning_container(message, key):
    """경고 메시지 컨테이너 생성"""
    warning_container = st.container()
    with warning_container:
        col1, col2 = st.columns([20, 1])
        with col1:
            st.error(message)
        with col2:
            if st.button("✕", key=key, help="닫기"):
                warning_container.empty()
    return warning_container

def create_success_container(message, key):
    """성공 메시지 컨테이너 생성"""
    success_container = st.container()
    with success_container:
        col1, col2 = st.columns([20, 1])
        with col1:
            st.success(message)
        with col2:
            if st.button("✕", key=key, help="닫기"):
                success_container.empty()
    return success_container

# =============================================================================
# 데이터 로드 함수들
# =============================================================================

def load_influencer_data():
    """인플루언서 데이터 로드"""
    if os.path.exists(INFLUENCER_FILE):
        df = pd.read_csv(INFLUENCER_FILE, encoding="utf-8")
        df.columns = df.columns.str.strip()
        return df
    else:
        st.error("인플루언서 데이터 파일이 없습니다.")
        return None

def pull_latest_data_from_github(show_in_sidebar=False):
    """GitHub에서 최신 데이터 가져오기"""
    # 로컬 환경에서는 실행하지 않음
    if not is_running_on_streamlit_cloud():
        if show_in_sidebar:
            st.sidebar.info("💻 로컬 환경에서는 GitHub 동기화가 비활성화됩니다.")
        else:
            st.info("💻 로컬 환경에서는 GitHub 동기화가 비활성화됩니다.")
        return False
    
    try:
        # Git pull 실행
        result = subprocess.run(['git', 'pull', 'origin', 'master'], 
                              capture_output=True, text=True, cwd=SCRIPT_DIR)
        
        if result.returncode == 0:
            if show_in_sidebar:
                st.sidebar.success("✅ GitHub에서 최신 데이터를 가져왔습니다!")
            else:
                st.success("✅ GitHub에서 최신 데이터를 가져왔습니다!")
            return True
        else:
            if show_in_sidebar:
                st.sidebar.warning(f"⚠️ GitHub에서 데이터 가져오기 실패: {result.stderr}")
            else:
                st.warning(f"⚠️ GitHub에서 데이터 가져오기 실패: {result.stderr}")
            return False
            
    except Exception as e:
        if show_in_sidebar:
            st.sidebar.warning(f"⚠️ GitHub 데이터 가져오기 중 오류: {e}")
        else:
            st.warning(f"⚠️ GitHub 데이터 가져오기 중 오류: {e}")
        return False

def load_assignment_history():
    """배정 이력 로드"""
    if os.path.exists(ASSIGNMENT_FILE):
        return pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
    return pd.DataFrame()

def load_execution_data():
    """실행 데이터 로드"""
    if os.path.exists(EXECUTION_FILE):
        return pd.read_csv(EXECUTION_FILE, encoding="utf-8")
    return pd.DataFrame()

# =============================================================================
# 검증 함수들
# =============================================================================

def check_previous_month_completion(selected_month, selected_season, df):
    """이전 달 배정 완료 상태 확인"""
    month_order = get_month_options(selected_season)
    selected_month_index = month_order.index(selected_month)
    
    if selected_month_index > 0:
        previous_month = month_order[selected_month_index - 1]
        existing_history = load_assignment_history()
        previous_month_assignments = existing_history[existing_history["배정월"] == previous_month] if not existing_history.empty else pd.DataFrame()
        
        if not previous_month_assignments.empty:
            incomplete_assignments = []
            execution_data = load_execution_data()
            
            for _, assignment in previous_month_assignments.iterrows():
                # execution_data가 비어있거나 필요한 컬럼이 없으면 모든 배정을 미완료로 처리
                if execution_data.empty or 'id' not in execution_data.columns:
                    incomplete_assignments.append(f"{assignment['이름']} ({assignment['브랜드']})")
                else:
                    exec_mask = (
                        (execution_data['id'] == assignment['id']) &
                        (execution_data['브랜드'] == assignment['브랜드']) &
                        (execution_data['배정월'] == assignment['배정월'])
                    )
                    
                    # 해당 배정에 대한 집행 데이터가 없거나 실제집행수가 0이면 집행상태 미업데이트
                    if not exec_mask.any():
                        incomplete_assignments.append(f"{assignment['이름']} ({assignment['브랜드']}) - 집행상태 미업데이트")
                    elif execution_data.loc[exec_mask, '실제집행수'].iloc[0] == 0:
                        incomplete_assignments.append(f"{assignment['이름']} ({assignment['브랜드']}) - 집행상태 미업데이트")
            
            if incomplete_assignments:
                return False, incomplete_assignments, previous_month
    
    return True, [], None

def display_incomplete_assignments(incomplete_assignments, previous_month, df):
    """미완료 배정 목록 표시"""
    st.error(f"❌ {previous_month} 배정된 인플루언서 중 집행상태가 업데이트되지 않은 배정이 있습니다. 모든 이전 달 집행상태가 업데이트된 상태여야 다음 달 배정이 가능합니다.")
    
    if st.button("🔙 돌아가기", type="secondary", use_container_width=True):
        st.session_state.go_back_clicked = True
    
    # 브랜드별로 상태 정리
    brand_assignments = {"MLB": [], "DX": [], "DV": [], "ST": []}
    for assignment in incomplete_assignments:
        if "(" in assignment and ")" in assignment:
            brand = assignment.split("(")[1].split(")")[0]
            if brand in brand_assignments:
                brand_assignments[brand].append(assignment.split(" (")[0])
    
    # 브랜드별로 상태 표시
    for brand in BRANDS:
        if brand_assignments[brand]:
            st.markdown(f'<div class="brand-list"><div class="brand-title">{brand}</div>', unsafe_allow_html=True)
            for name in brand_assignments[brand]:
                influencer_id = df[df['name'] == name]['id'].iloc[0] if not df[df['name'] == name].empty else "ID 없음"
                st.markdown(f'<div class="influencer-item">• {name}, {influencer_id}</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
    
    st.stop()

# =============================================================================
# 공통 유틸리티 함수들
# =============================================================================

def calculate_remaining_quantity(influencer_id, brand, df):
    """잔여수 계산 공통 함수 (브랜드_집행수 기반)"""
    # 인플루언서 데이터 확인
    influencer_data = df[df['id'] == influencer_id]
    if influencer_data.empty:
        return 0
    
    brand_qty_col = f"{brand.lower()}_qty"
    brand_contract_qty = influencer_data.iloc[0].get(brand_qty_col, 0)
    
    # 1. 브랜드_집행수 확인 (사용자가 직접 입력한 값)
    brand_execution_count = influencer_data.iloc[0].get('브랜드_집행수', 0)
    
    # 2. 현재까지의 모든 배정 수 확인
    assignment_history = load_assignment_history()
    total_assigned_count = 0
    if not assignment_history.empty and 'id' in assignment_history.columns and '브랜드' in assignment_history.columns:
        existing_assignments = assignment_history[
            (assignment_history['id'] == influencer_id) & 
            (assignment_history['브랜드'] == brand)
        ]
        total_assigned_count = len(existing_assignments)
    
    # 3. 실제 잔여수 계산: 계약수 - (브랜드_집행수 + 배정)
    actual_remaining = max(0, brand_contract_qty - brand_execution_count - total_assigned_count)
    return actual_remaining

def check_duplicate_assignment(influencer_id, brand, month, assignment_history):
    """중복 배정 체크 공통 함수"""
    if assignment_history.empty:
        return False
    
    existing_mask = (
        (assignment_history['id'] == influencer_id) &
        (assignment_history['브랜드'] == brand) &
        (assignment_history['배정월'] == month)
    )
    return existing_mask.any()

def calculate_brand_remaining_quantity(influencer_id, brand, df):
    """브랜드별 잔여수 계산"""
    return calculate_remaining_quantity(influencer_id, brand, df)

def calculate_total_remaining_quantity(influencer_id, df):
    """전체 잔여수 계산"""
    influencer_data = df[df['id'] == influencer_id]
    if influencer_data.empty:
        return 0
    
    # 전체 계약수
    total_contract_qty = (
        influencer_data.iloc[0].get('mlb_qty', 0) + 
        influencer_data.iloc[0].get('dx_qty', 0) + 
        influencer_data.iloc[0].get('dv_qty', 0) + 
        influencer_data.iloc[0].get('st_qty', 0)
    )
    
    # 전체 집행완료 수
    execution_data = load_execution_data()
    total_executed_count = 0
    if not execution_data.empty:
        exec_mask = (execution_data['id'] == influencer_id)
        if exec_mask.any():
            total_executed_count = execution_data.loc[exec_mask, '실제집행수'].sum()
    
    # 전체 배정 수
    assignment_history = load_assignment_history()
    total_assigned_count = 0
    if not assignment_history.empty and 'id' in assignment_history.columns:
        existing_assignments = assignment_history[assignment_history['id'] == influencer_id]
        total_assigned_count = len(existing_assignments)
    
    # 전체 잔여수 계산
    total_remaining = max(0, total_contract_qty - total_executed_count - total_assigned_count)
    return total_remaining

# =============================================================================
# 배정 관련 함수들
# =============================================================================

def execute_automatic_assignment(selected_month, selected_season, quantities, df, skip_previous_check=False):
    """자동 배정 실행"""
    # 이전 달 완료 상태 확인 (skip_previous_check가 True면 건너뛰기)
    if not skip_previous_check:
        is_complete, incomplete_assignments, previous_month = check_previous_month_completion(selected_month, selected_season, df)
        
        if not is_complete:
            display_incomplete_assignments(incomplete_assignments, previous_month, df)
            return
    
    # 기존 배정 확인
    existing_history = load_assignment_history()
    selected_month_assignments = existing_history[existing_history["배정월"] == selected_month] if not existing_history.empty else pd.DataFrame()
    already_assigned_influencers = set(selected_month_assignments["id"].unique()) if not selected_month_assignments.empty and "id" in selected_month_assignments.columns else set()
    
    # 같은 브랜드로 이미 배정된 경우 확인
    existing_brand_assignments = {}
    if not selected_month_assignments.empty:
        for _, row in selected_month_assignments.iterrows():
            brand = row['브랜드']
            influencer_id = row['id']
            influencer_name = row['이름']
            if brand not in existing_brand_assignments:
                existing_brand_assignments[brand] = []
            existing_brand_assignments[brand].append(f"{influencer_name} ({influencer_id})")
    
    # 배정할 브랜드 중 이미 배정된 브랜드가 있는지 확인
    conflicting_brands = []
    for brand, qty in quantities.items():
        if qty > 0 and brand in existing_brand_assignments:
            conflicting_brands.append(brand)
    
    if conflicting_brands:
        st.warning(f"⚠️ {selected_month}에 이미 배정된 브랜드가 있습니다. 기존 배정에 추가로 배정합니다.")
    
    # 중복 알림 제거 - 위의 warning으로 충분함
    
    # 배정 로직 실행
    results = []
    newly_assigned_influencers = set()
    
    for brand, qty in quantities.items():
        if qty > 0:
            brand_df = df[df[f"{brand.lower()}_qty"] > 0].copy()
            brand_df = brand_df[~brand_df["id"].isin(already_assigned_influencers)]
            brand_df = brand_df[~brand_df["id"].isin(newly_assigned_influencers)]
            
            # 잔여계약수가 많은 순서로 우선 정렬
            # 각 인플루언서의 잔여계약수 계산
            brand_df['잔여계약수'] = brand_df.apply(
                lambda row: calculate_remaining_quantity(row['id'], brand, df), axis=1
            )
            
            # 잔여계약수가 많은 순서로 정렬, 같은 잔여계약수면 랜덤 배정
            brand_df = brand_df.sort_values('잔여계약수', ascending=False)
            # 같은 잔여계약수 내에서는 랜덤 순서로 배정
            brand_df = brand_df.sample(frac=1, random_state=42).reset_index(drop=True)
            
            assigned_count = 0
            for _, row in brand_df.iterrows():
                if assigned_count >= qty:
                    break
                
                # 배정 핵심 로직: 공통 함수 사용
                actual_remaining = calculate_remaining_quantity(row['id'], brand, df)
                
                # 잔여수가 없으면 배정 불가
                if actual_remaining <= 0:
                    continue  # 잔여수가 없으면 건너뛰기
                
                # 배정 정보 생성
                assignment_info = create_assignment_info(row, brand, selected_month, df)
                results.append(assignment_info)
                
                newly_assigned_influencers.add(row["id"])
                assigned_count += 1
    
    # 상태 저장
    if results:
        save_assignments(results, existing_history)
        
        # 성공 메시지를 컨테이너로 감싸서 3초 후 자동 제거
        success_container = st.container()
        with success_container:
            st.success(f"✅ {selected_month}에 {len(results)}개의 배정이 완료되었습니다!")
        
        # 배정수량관리 탭에서는 rerun하지 않음 (다른 월 배정을 위해)
        if not skip_previous_check:
            # 사용자가 알림을 읽을 수 있도록 3초 대기
            time.sleep(3)
            st.rerun()
        else:
            # 배정수량관리 탭에서는 3초 후 메시지 자동 제거
            time.sleep(3)
            success_container.empty()
    else:
        warning_container = st.container()
        with warning_container:
            st.warning(f"⚠️ {selected_month}에 배정할 수 있는 인플루언서가 없습니다.")
        
        # 배정수량관리 탭에서는 rerun하지 않음
        if not skip_previous_check:
            # 사용자가 알림을 읽을 수 있도록 3초 대기
            time.sleep(3)
        else:
            # 배정수량관리 탭에서는 3초 후 메시지 자동 제거
            time.sleep(3)
            warning_container.empty()

def create_assignment_info(row, brand, selected_month, df):
    """배정 정보 생성"""
    original_brand_qty = df.loc[df["id"] == row["id"], f"{brand.lower()}_qty"].iloc[0]
    original_total_qty = df.loc[df["id"] == row["id"], ["mlb_qty", "dx_qty", "dv_qty", "st_qty"]].sum().iloc[0]
    
    # 실행 데이터 확인
    execution_data = load_execution_data()
    brand_execution_count = 0
    total_execution_count = 0
    
    if not execution_data.empty and 'id' in execution_data.columns and '브랜드' in execution_data.columns:
        exec_mask = (
            (execution_data['id'] == row['id']) &
            (execution_data['브랜드'] == brand)
        )
        if exec_mask.any():
            brand_execution_count = execution_data.loc[exec_mask, '실제집행수'].sum()
        
        total_exec_mask = (execution_data['id'] == row['id'])
        if total_exec_mask.any():
            total_execution_count = execution_data.loc[total_exec_mask, '실제집행수'].sum()
    
    # 배정 데이터 확인
    assignment_data = load_assignment_history()
    brand_assignment_count = 0
    total_assignment_count = 0
    
    if not assignment_data.empty and 'id' in assignment_data.columns and '브랜드' in assignment_data.columns:
        # '상태' 컬럼이 있는지 확인하고, 없으면 모든 배정을 '배정완료'로 간주
        if '상태' in assignment_data.columns:
            assign_mask = (
                (assignment_data['id'] == row['id']) &
                (assignment_data['브랜드'] == brand) &
                (assignment_data['상태'] == '배정완료')
            )
            if assign_mask.any():
                brand_assignment_count = len(assignment_data.loc[assign_mask])
            
            total_assign_mask = (
                (assignment_data['id'] == row['id']) &
                (assignment_data['상태'] == '배정완료')
            )
            if total_assign_mask.any():
                total_assignment_count = len(assignment_data.loc[total_assign_mask])
        else:
            # '상태' 컬럼이 없으면 모든 배정을 '배정완료'로 간주
            assign_mask = (
                (assignment_data['id'] == row['id']) &
                (assignment_data['브랜드'] == brand)
            )
            if assign_mask.any():
                brand_assignment_count = len(assignment_data.loc[assign_mask])
            
            total_assign_mask = (assignment_data['id'] == row['id'])
            if total_assign_mask.any():
                total_assignment_count = len(assignment_data.loc[total_assign_mask])
    
    # 잔여수 계산 (계약수 - 집행완료 - 배정완료)
    brand_remaining = max(0, original_brand_qty - brand_execution_count - brand_assignment_count)
    total_remaining = max(0, original_total_qty - total_execution_count - total_assignment_count)
    
    return {
        "브랜드": brand,
        "id": row["id"],
        "이름": row["name"],
        "배정월": selected_month,
        "FLW": row["follower"],
        "1회계약단가": row["unit_fee"],
        "2차활용": row["sec_usage"],
        "브랜드_계약수": original_brand_qty,
        "브랜드_실집행수": brand_execution_count,
        "브랜드_잔여수": brand_remaining,
        "전체_계약수": original_total_qty,
        "전체_실집행수": total_execution_count,
        "전체_잔여수": total_remaining,
        "집행URL": "",
        "상태": "📋 배정완료"
    }

def save_assignments(new_assignments, existing_history):
    """배정 정보 저장"""
    result_df = pd.DataFrame(new_assignments)
    
    # 브랜드 필드 정리: 쉼표가 포함된 브랜드 값을 분리
    result_df = clean_brand_columns(result_df)
    
    if not existing_history.empty:
        # 기존 데이터도 정리
        existing_history = clean_brand_columns(existing_history)
        updated_history = pd.concat([existing_history, result_df], ignore_index=True)
    else:
        updated_history = result_df
    
    # 클라우드에서는 GitHub 동기화, 로컬에서는 로컬 저장만
    if is_running_on_streamlit_cloud():
        save_with_auto_sync(updated_history, ASSIGNMENT_FILE, "자동 배정 실행")
    else:
        save_local_only(updated_history, ASSIGNMENT_FILE)

def clean_brand_columns(df):
    """브랜드 컬럼 정리: 쉼표가 포함된 브랜드 값을 분리"""
    if '브랜드' not in df.columns:
        return df
    
    cleaned_rows = []
    for _, row in df.iterrows():
        brand = row['브랜드']
        if isinstance(brand, str) and ',' in brand:
            # 쉼표로 구분된 브랜드들을 분리
            brands = [b.strip() for b in brand.split(',')]
            for single_brand in brands:
                if single_brand in BRANDS:  # 유효한 브랜드인지 확인
                    new_row = row.copy()
                    new_row['브랜드'] = single_brand
                    cleaned_rows.append(new_row)
        else:
            cleaned_rows.append(row)
    
    # cleaned_rows가 비어있으면 원본 DataFrame 반환
    if cleaned_rows:
        return pd.DataFrame(cleaned_rows)
    else:
        return df

def execute_manual_assignment(selected_month, selected_season, brand, influencer_id, df):
    """수동 배정 실행"""
    # 이전 달 완료 상태 확인
    is_complete, incomplete_assignments, previous_month = check_previous_month_completion(selected_month, selected_season, df)
    
    if not is_complete:
        display_incomplete_assignments(incomplete_assignments, previous_month, df)
        return
    
    if influencer_id and influencer_id in df['id'].values:
        influencer_name = df[df['id'] == influencer_id]['name'].iloc[0]
        assignment_history = load_assignment_history()
        
        # 중복 배정 확인: 공통 함수 사용
        if not check_duplicate_assignment(influencer_id, brand, selected_month, assignment_history):
            # 배정 핵심 로직: 공통 함수 사용
            actual_remaining = calculate_remaining_quantity(influencer_id, brand, df)
            
            # 잔여수가 없으면 배정 불가
            if actual_remaining <= 0:
                influencer_data = df[df['id'] == influencer_id].iloc[0]
                brand_qty_col = f"{brand.lower()}_qty"
                brand_contract_qty = influencer_data.get(brand_qty_col, 0)
                
                # 집행완료 수와 배정 수 계산 (에러 메시지용)
                execution_data = load_execution_data()
                total_executed_count = 0
                if not execution_data.empty:
                    exec_mask = (
                        (execution_data['id'] == influencer_id) &
                        (execution_data['브랜드'] == brand)
                    )
                    if exec_mask.any():
                        total_executed_count = execution_data.loc[exec_mask, '실제집행수'].sum()
                
                existing_assignments = assignment_history[
                    (assignment_history['id'] == influencer_id) & 
                    (assignment_history['브랜드'] == brand)
                ]
                total_assigned_count = len(existing_assignments)
                
                st.sidebar.error(f"❌ {influencer_name}의 {brand} 브랜드 잔여수가 없습니다. (계약수: {brand_contract_qty}, 집행완료: {total_executed_count}, 배정: {total_assigned_count})")
                return
            
            # 새로운 배정 추가
            new_assignment = create_manual_assignment_info(influencer_id, brand, selected_month, df)
            assignment_history = pd.concat([assignment_history, pd.DataFrame([new_assignment])], ignore_index=True)
            # 클라우드에서는 GitHub 동기화, 로컬에서는 로컬 저장만
            if is_running_on_streamlit_cloud():
                save_with_auto_sync(assignment_history, ASSIGNMENT_FILE, "수동 배정 추가")
            else:
                save_local_only(assignment_history, ASSIGNMENT_FILE)
            
            if 'selected_id' in st.session_state:
                st.session_state.selected_id = ""
            
            st.rerun()
        else:
            st.sidebar.warning(f"⚠️ {influencer_name}의 {selected_month} {brand} 배정이 이미 존재합니다.")
    else:
        st.sidebar.error("❌ 올바른 인플루언서 ID를 입력해주세요.")

def create_manual_assignment_info(influencer_id, brand, selected_month, df):
    """수동 배정 정보 생성"""
    influencer_data = df[df['id'] == influencer_id].iloc[0]
    brand_qty_col = f"{brand.lower()}_qty"
    brand_contract_qty = influencer_data.get(brand_qty_col, 0)
    
    total_contract_qty = (influencer_data.get('mlb_qty', 0) + 
                         influencer_data.get('dx_qty', 0) + 
                         influencer_data.get('dv_qty', 0) + 
                         influencer_data.get('st_qty', 0))
    
    # 기존 집행 및 배정 데이터 확인
    execution_data = load_execution_data()
    assignment_data = load_assignment_history()
    
    # 브랜드별 집행수 계산
    brand_execution_count = 0
    total_execution_count = 0
    if not execution_data.empty and 'id' in execution_data.columns and '브랜드' in execution_data.columns:
        exec_mask = (
            (execution_data['id'] == influencer_id) &
            (execution_data['브랜드'] == brand)
        )
        if exec_mask.any():
            brand_execution_count = execution_data.loc[exec_mask, '실제집행수'].sum()
        
        total_exec_mask = (execution_data['id'] == influencer_id)
        if total_exec_mask.any():
            total_execution_count = execution_data.loc[total_exec_mask, '실제집행수'].sum()
    
    # 브랜드별 배정수 계산
    brand_assignment_count = 0
    total_assignment_count = 0
    if not assignment_data.empty and 'id' in assignment_data.columns and '브랜드' in assignment_data.columns:
        assign_mask = (
            (assignment_data['id'] == influencer_id) &
            (assignment_data['브랜드'] == brand) &
            (assignment_data['상태'] == '배정완료')
        )
        if assign_mask.any():
            brand_assignment_count = len(assignment_data.loc[assign_mask])
        
        total_assign_mask = (
            (assignment_data['id'] == influencer_id) &
            (assignment_data['상태'] == '배정완료')
        )
        if total_assign_mask.any():
            total_assignment_count = len(assignment_data.loc[total_assign_mask])
    
    # 잔여수 계산 (계약수 - 집행완료 - 배정완료)
    brand_remaining = max(0, brand_contract_qty - brand_execution_count - brand_assignment_count)
    total_remaining = max(0, total_contract_qty - total_execution_count - total_assignment_count)
    
    return {
        '브랜드': brand,
        'id': influencer_id,
        '이름': influencer_data['name'],
        '배정월': selected_month,
        'FLW': influencer_data['follower'],
        '1회계약단가': influencer_data['unit_fee'],
        '2차활용': influencer_data['sec_usage'],
        '브랜드_계약수': brand_contract_qty,
        '브랜드_실집행수': brand_execution_count,
        '브랜드_잔여수': brand_remaining,
        '전체_계약수': total_contract_qty,
        '전체_실집행수': total_execution_count,
        '전체_잔여수': total_remaining,
        '집행URL': ""
    }

# =============================================================================
# UI 컴포넌트 함수들
# =============================================================================

def render_sidebar(df):
    """사이드바 렌더링"""
    st.sidebar.header("📋 배정 설정")
    
    # 시즌 및 월 선택
    selected_season = st.sidebar.selectbox("시즌", SEASON_OPTIONS, key="sidebar_season")
    month_options = get_month_options(selected_season)
    selected_month = st.sidebar.selectbox("배정월", month_options, key="sidebar_month")
    
    # 자동 배정 수량
    st.sidebar.markdown("<hr style='margin: 10px 0; border: 0.5px solid #666;'>", unsafe_allow_html=True)
    st.sidebar.subheader("🎯 자동 배정 수량")
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        mlb_qty = st.number_input("MLB", min_value=0, value=0)
        dv_qty = st.number_input("DV", min_value=0, value=0)
    with col2:
        dx_qty = st.number_input("DX", min_value=0, value=0)
        st_qty = st.number_input("ST", min_value=0, value=0)
    
    quantities = {"MLB": mlb_qty, "DX": dx_qty, "DV": dv_qty, "ST": st_qty}
    
    # 자동 배정 실행
    if st.sidebar.button("🚀 자동 배정실행", type="primary", use_container_width=True):
        execute_automatic_assignment(selected_month, selected_season, quantities, df)
    
    # 수동 배정
    render_manual_assignment_section(selected_month, selected_season, df)
    
    # 선택된 월을 session_state에 저장
    st.session_state.selected_month = selected_month
    
    # GitHub 동기화 상태 확인 (사이드바 맨 하단에 배치)
    st.sidebar.markdown("<hr style='margin: 10px 0; border: 0.5px solid #666;'>", unsafe_allow_html=True)
    if st.sidebar.button("🔍 GitHub 동기화 상태 확인", key="github_sync_check", use_container_width=True):
        # GitHub 연결 상태 확인
        connection_status = check_github_connection()
        
        # 클라우드에서 실행 중인 경우에만 GitHub 동기화 상태 확인
        if connection_status and is_running_on_streamlit_cloud():
            check_github_sync_status()
    
    return selected_month, selected_season, month_options

def render_manual_assignment_section(selected_month, selected_season, df):
    """수동 배정 섹션 렌더링"""
    st.sidebar.markdown("<hr style='margin: 10px 0; border: 0.5px solid #666;'>", unsafe_allow_html=True)
    st.sidebar.subheader("➕ 수동 배정 추가")
    
    # 배정 브랜드 선택
    manual_assignment_brand = st.sidebar.selectbox(
        "🏷️ 배정 브랜드",
        BRANDS,
        key="manual_assignment_brand"
    )
    
    # ID 입력
    default_id = st.session_state.get('selected_id', st.session_state.get('manual_assignment_id', ""))
    manual_assignment_id = st.sidebar.text_input(
        "👤 인플루언서 ID",
        value=default_id,
        key="manual_assignment_id",
        help="ID를 입력하면 유사한 ID 목록이 표시됩니다"
    )
    
    # 수동 배정 저장
    if st.sidebar.button("💾 수동 배정저장", type="primary", use_container_width=True):
        execute_manual_assignment(selected_month, selected_season, manual_assignment_brand, manual_assignment_id, df)
    
    # ID 추천 목록
    render_id_suggestions(manual_assignment_id, df)
    render_selected_id_info()

def render_id_suggestions(manual_assignment_id, df):
    """ID 추천 목록 렌더링"""
    if manual_assignment_id:
        similar_ids = df[df['id'].str.lower().str.startswith(manual_assignment_id.lower(), na=False)]['id'].tolist()
        if similar_ids:
            st.sidebar.markdown("**유사한 ID 목록:**")
            for similar_id in similar_ids[:3]:
                if st.sidebar.button(f"선택: {similar_id}", key=f"select_id_{similar_id}"):
                    st.session_state.selected_id = similar_id
                    st.session_state.id_selected = True
    
    # 선택된 ID가 있을 때 다른 유사한 ID 목록
    if 'selected_id' in st.session_state and st.session_state.selected_id:
        selected_id = st.session_state.selected_id
        first_char = selected_id[0].lower()
        similar_ids = df[df['id'].str.lower().str.startswith(first_char, na=False)]['id'].tolist()
        other_similar_ids = [id for id in similar_ids if id != selected_id]
        
        if other_similar_ids:
            st.sidebar.markdown("**다른 유사한 ID 목록:**")
            for similar_id in other_similar_ids[:3]:
                if st.sidebar.button(f"선택: {similar_id}", key=f"select_other_id_{similar_id}"):
                    st.session_state.selected_id = similar_id
                    st.session_state.other_id_selected = True

def render_selected_id_info():
    """선택된 ID 정보 렌더링"""
    if 'selected_id' in st.session_state and st.session_state.selected_id:
        selected_id = st.session_state.selected_id
        
        # 인플루언서 상세 정보 가져오기
        influencer_info = get_influencer_info(selected_id)
        
        if influencer_info is not None:
            info_container = st.sidebar.container()
            with info_container:
                # 선택 확인 메시지
                col1, col2 = st.columns([20, 1])
                with col1:
                    st.sidebar.success(f"✅ {selected_id} 선택됨!")
                with col2:
                    if st.sidebar.button("✕", key="close_selected_id_info", help="닫기"):
                        st.session_state.selected_id = ""
                        st.session_state.id_info_closed = True
                
                # 인플루언서 상세 정보 표시
                st.sidebar.markdown("---")
                st.sidebar.markdown("**👤 인플루언서 정보:**")
                
                # 정보를 컬럼으로 표시
                col1, col2 = st.sidebar.columns(2)
                with col1:
                    st.sidebar.markdown(f"**이름:** {influencer_info['name']}")
                    st.sidebar.markdown(f"**팔로워:** {influencer_info['follower']:,}")
                with col2:
                    st.sidebar.markdown(f"**1회단가:** {influencer_info['unit_fee']:,}원")
                    st.sidebar.markdown(f"**소속사:** {influencer_info['agency'] or '개인'}")
                
                # 브랜드별 계약 수량 정보
                st.sidebar.markdown("**📊 브랜드별 계약:**")
                brand_cols = st.sidebar.columns(4)
                brands = ['MLB', 'DX', 'DV', 'ST']
                for i, brand in enumerate(brands):
                    qty_col = f"{brand.lower()}_qty"
                    if qty_col in influencer_info:
                        with brand_cols[i]:
                            st.sidebar.markdown(f"**{brand}:** {influencer_info[qty_col]}")
                    else:
                        with brand_cols[i]:
                            st.sidebar.markdown(f"**{brand}:** 0")
        else:
            st.sidebar.error(f"❌ {selected_id} 정보를 찾을 수 없습니다.")

def render_assignment_results_tab(month_options, df):
    """배정 및 집행상태 탭 렌더링"""
    st.subheader("📊 배정 및 집행상태")
    
    # 필터
    month_options_with_all = ["전체"] + month_options
    selected_month_filter = st.selectbox("📅 배정월", month_options_with_all, index=0, key="tab1_month_filter")
    selected_brand_filter = st.selectbox("🏷️ 브랜드", BRAND_OPTIONS, index=0, key="tab1_brand_filter")
    
    # 배정 상태 로드 및 표시
    if os.path.exists(ASSIGNMENT_FILE):
        assignment_history = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        
        if not assignment_history.empty:
            # 실행 데이터 추가
            all_results = add_execution_data(assignment_history, EXECUTION_FILE)
            
            # 필터 적용
            if selected_month_filter != "전체":
                all_results = all_results[all_results["배정월"] == selected_month_filter]
            if selected_brand_filter != "전체":
                all_results = all_results[all_results["브랜드"] == selected_brand_filter]
            
            # 브랜드 필터 선택 시 컬럼 변경
            if selected_brand_filter != "전체":
                # 브랜드 필터 선택 시: 브랜드_잔여수 삭제, 브랜드_집행수 추가
                expected_columns = ["브랜드", "id", "이름", "배정월", "FLW", "브랜드_계약수", 
                                  "브랜드_집행수", "전체_계약수", "전체_잔여수"]
            else:
                # 전체 브랜드 선택 시: 브랜드_잔여수 유지
                expected_columns = ["브랜드", "id", "이름", "배정월", "FLW", "브랜드_계약수", 
                                  "브랜드_잔여수", "전체_계약수", "전체_잔여수"]
            
            all_results = reorder_columns(all_results, expected_columns)
            
            if not all_results.empty:
                render_assignment_table(all_results, df)
            else:
                st.info("해당 조건의 배정 상태가 없습니다.")
        else:
            st.info("배정 이력이 없습니다.")
    else:
        st.info("배정 이력이 없습니다.")
    
    # 엑셀 업로드 섹션
    render_excel_upload_section(df)

def render_assignment_table(all_results, df):
    """배정 테이블 렌더링"""
    # 체크박스, 넘버, 상태 상태 추가
    all_results_with_checkbox = prepare_assignment_data(all_results)
    
    # 배정 개수 정보 표시
    assignment_count = len(all_results_with_checkbox)
    st.markdown(f"📊 배정 개수: **{assignment_count}개**")
    
    # 전체 선택/해제 버튼과 다운로드 버튼
    render_table_controls(all_results_with_checkbox)
    
    # 데이터프레임 표시
    edited_df = render_data_editor(all_results_with_checkbox)
    
    # 변경사항 처리
    handle_assignment_changes(edited_df, all_results_with_checkbox, df)
    
    # 하단 버튼들
    render_assignment_buttons(edited_df, df)

def prepare_assignment_data(all_results):
    """배정 데이터 준비"""
    all_results_with_checkbox = all_results.copy()
    
    # 전체 선택 상태에 따라 체크박스 기본값 설정
    default_checked = st.session_state.get('select_all', False)
    all_results_with_checkbox['선택'] = default_checked
    all_results_with_checkbox['번호'] = range(1, len(all_results_with_checkbox) + 1)
    
    # 상태 컬럼이 없으면 기본값으로 초기화
    if '상태' not in all_results_with_checkbox.columns:
        all_results_with_checkbox['상태'] = '📋 배정완료'
    else:
        # 상태 컬럼이 있으면 빈 값만 기본값으로 설정
        all_results_with_checkbox['상태'] = all_results_with_checkbox['상태'].fillna('📋 배정완료')
    
    # 기존 배정 이력에서 상태 값 가져오기
    load_existing_results(all_results_with_checkbox)
    
    # 실집행수가 있는 경우 '집행완료'로 변경
    update_execution_status(all_results_with_checkbox)
    
    # 숫자 컬럼 처리
    process_numeric_columns(all_results_with_checkbox)
    
    # 집행URL 컬럼 추가 및 기존 데이터 로드
    add_execution_url_column(all_results_with_checkbox)
    
    # 화면 표시용으로 브랜드_실집행수, 전체_계약수, 전체_잔여수 컬럼 제거 (브랜드_잔여수는 유지)
    columns_to_remove = ['브랜드_실집행수', '전체_계약수', '전체_잔여수']
    for col in columns_to_remove:
        if col in all_results_with_checkbox.columns:
            all_results_with_checkbox = all_results_with_checkbox.drop(col, axis=1)
    
    # 항상 influencer.csv에서 기본 정보 가져오기 (배정 이력과 관계없이)
    influencer_data = pd.read_csv(INFLUENCER_FILE, encoding="utf-8")
    
    # 1회계약단가, 2차활용, 2차기간은 항상 influencer.csv에서 가져옴
    unit_fee_mapping = dict(zip(influencer_data['id'], influencer_data['unit_fee']))
    sec_usage_mapping = dict(zip(influencer_data['id'], influencer_data['sec_usage']))
    sec_period_mapping = dict(zip(influencer_data['id'], influencer_data['sec_period']))
    
    all_results_with_checkbox['1회계약단가'] = all_results_with_checkbox['id'].map(unit_fee_mapping).fillna(0)
    all_results_with_checkbox['2차활용'] = all_results_with_checkbox['id'].map(sec_usage_mapping).fillna('X')
    all_results_with_checkbox['2차기간'] = all_results_with_checkbox['id'].map(sec_period_mapping).fillna('')
    
        # 컬럼 순서 재정렬 (2차활용 다음에 2차기간, 브랜드_잔여수를 브랜드_계약수 다음에, 상태를 맨 오른쪽에 배치)
    cols = ['선택', '번호', '배정월', '브랜드', 'id', '이름', 'FLW', '1회계약단가', '2차활용', '2차기간', '브랜드_계약수', '브랜드_잔여수', '상태', '집행URL']
    # 존재하는 컬럼만 필터링
    existing_cols = [col for col in cols if col in all_results_with_checkbox.columns]
    # 나머지 컬럼들 추가
    remaining_cols = [col for col in all_results_with_checkbox.columns if col not in existing_cols]
    all_results_with_checkbox = all_results_with_checkbox[existing_cols + remaining_cols]
    
    return all_results_with_checkbox

def load_existing_results(all_results_with_checkbox):
    """기존 배정 이력에서 상태 값 가져오기 (엑셀 업로드 데이터 우선)"""
    # 기존 배정 이력에서 상태 값 가져오기 (엑셀에서 업로드한 데이터가 우선)
    if os.path.exists(ASSIGNMENT_FILE):
        assignment_history = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        if '상태' in assignment_history.columns:
            for idx, row in all_results_with_checkbox.iterrows():
                result_mask = (
                    (assignment_history['id'] == row['id']) &
                    (assignment_history['브랜드'] == row['브랜드']) &
                    (assignment_history['배정월'] == row['배정월'])
                )
                if result_mask.any():
                    result_value = assignment_history.loc[result_mask, '상태'].iloc[0]
                    # 엑셀에서 업로드한 상태 값이 있으면 그것을 우선시
                    if pd.notna(result_value) and result_value != "":
                        # 상태 값 변환 (이모지 형태로 통일)
                        if result_value == '배정완료':
                            all_results_with_checkbox.loc[idx, '상태'] = '📋 배정완료'
                        elif result_value == '집행완료':
                            all_results_with_checkbox.loc[idx, '상태'] = '✅ 집행완료'
                        else:
                            # 이미 이모지가 포함된 경우 그대로 사용
                            all_results_with_checkbox.loc[idx, '상태'] = result_value

def update_execution_status(all_results_with_checkbox):
    """실행 상태 업데이트"""
    if os.path.exists(EXECUTION_FILE):
        execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
        if not execution_data.empty:
            for idx, row in all_results_with_checkbox.iterrows():
                exec_mask = (
                    (execution_data['id'] == row['id']) &
                    (execution_data['브랜드'] == row['브랜드']) &
                    (execution_data['배정월'] == row['배정월'])
                )
                if exec_mask.any() and execution_data.loc[exec_mask, '실제집행수'].iloc[0] > 0:
                    # 기존 상태가 '📋 배정완료'인 경우에만 '✅ 집행완료'로 변경
                    # (엑셀에서 업로드한 다른 상태 값들은 유지)
                    if all_results_with_checkbox.loc[idx, '상태'] == '📋 배정완료':
                        all_results_with_checkbox.loc[idx, '상태'] = '✅ 집행완료'

def process_numeric_columns(all_results_with_checkbox):
    """숫자 컬럼 처리"""
    # 모든 숫자 컬럼을 정수형으로 유지 (문자열로 변환하지 않음)
    numeric_columns = ['브랜드_계약수', 'FLW', '1회계약단가', '브랜드_잔여수']
    for col in numeric_columns:
        if col in all_results_with_checkbox.columns:
            all_results_with_checkbox[col] = all_results_with_checkbox[col].fillna(0).astype(int)

def add_execution_url_column(all_results_with_checkbox):
    """집행URL 컬럼 추가"""
    all_results_with_checkbox['집행URL'] = ""
    
    if os.path.exists(ASSIGNMENT_FILE):
        assignment_history = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        if '집행URL' in assignment_history.columns:
            for idx, row in all_results_with_checkbox.iterrows():
                url_mask = (
                    (assignment_history['id'] == row['id']) &
                    (assignment_history['브랜드'] == row['브랜드']) &
                    (assignment_history['배정월'] == row['배정월'])
                )
                if url_mask.any():
                    url_value = assignment_history.loc[url_mask, '집행URL'].iloc[0]
                    if pd.notna(url_value) and url_value != "":
                        all_results_with_checkbox.loc[idx, '집행URL'] = url_value

def render_table_controls(all_results):
    """테이블 컨트롤 렌더링"""
    # 하단 버튼들과 정확히 같은 너비로 배치
    col1, col2, col3, col_spacer, col4 = st.columns([0.15, 0.15, 0.15, 0.1, 0.45])
    
    with col1:
        # 전체 선택 상태에 따라 버튼 텍스트 변경
        select_all_state = st.session_state.get('select_all', False)
        button_text = "✅ 전체선택" if not select_all_state else "✅ 전체해제"
        
        if st.button(button_text, type="secondary", use_container_width=True, key="select_all_button"):
            if 'select_all' not in st.session_state:
                st.session_state.select_all = True
            else:
                st.session_state.select_all = not st.session_state.select_all
            # 데이터 에디터 키를 변경하여 강제로 새로고침
            if 'data_editor_key' not in st.session_state:
                st.session_state.data_editor_key = 0
            st.session_state.data_editor_key += 1
            st.rerun()
    
    with col2:
        # 다운로드 버튼은 체크박스가 포함된 데이터가 필요하므로 임시로 준비
        temp_data = all_results.copy()
        temp_data['선택'] = st.session_state.get('select_all', False)
        temp_data['번호'] = range(1, len(temp_data) + 1)
        render_download_button(temp_data)
    
    with col3:
        pass  # 빈 공간
    
    with col4:
        pass  # 빈 공간

def render_download_button(all_results_with_checkbox):
    """다운로드 버튼 렌더링"""
    # 요청된 순서: 배정월/브랜드/ID/이름/FLW/2차활용/2차기간/상태/집행URL
    available_columns = ['배정월', '브랜드', 'id', '이름', 'FLW', '2차활용', '2차기간', '상태', '집행URL']
    
    # 누락된 컬럼들을 기본값으로 추가
    download_data = all_results_with_checkbox.copy()
    
    # 2차활용 컬럼이 없으면 기본값 'X'로 추가
    if '2차활용' not in download_data.columns:
        download_data['2차활용'] = 'X'
    
    # 2차기간 컬럼이 없으면 기본값 ''로 추가
    if '2차기간' not in download_data.columns:
        download_data['2차기간'] = ''
    
    # 상태 컬럼이 없으면 기본값 '배정완료'로 추가
    if '상태' not in download_data.columns:
        download_data['상태'] = '배정완료'
    
    # 집행URL 컬럼이 없으면 기본값 ''로 추가
    if '집행URL' not in download_data.columns:
        download_data['집행URL'] = ''
    
    # 요청된 순서대로 컬럼 선택
    existing_columns = [col for col in available_columns if col in download_data.columns]
    download_data = download_data[existing_columns].copy()
    
    if '상태' in download_data.columns:
        download_data['상태'] = download_data['상태'].replace({
            '📋 배정완료': '배정완료',
            '✅ 집행완료': '집행완료'
        })
    
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"assignment_results_{current_time}.xlsx"
    st.download_button(
        "📥 엑셀 다운로드",
        to_excel_bytes(download_data),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key="excel_download_button"
    )

def render_data_editor(all_results_with_checkbox):
    """데이터 에디터 렌더링"""
    # 동적 키 생성으로 강제 새로고침
    editor_key = f"assignment_data_editor_{st.session_state.get('data_editor_key', 0)}"
    
    return st.data_editor(
        all_results_with_checkbox,
        use_container_width=True,
        hide_index=True,
        key=editor_key,
        column_config={
            "선택": st.column_config.CheckboxColumn(
                "선택",
                help="실집행완료할 배정을 선택하세요",
                width=10
            ),
            "번호": st.column_config.NumberColumn(
                "번호",
                width=10,
                help="순서 번호",
                format="%d"
            ),
            "상태": st.column_config.SelectboxColumn(
                "상태",
                help="배정/집행 상태 (직접 변경 가능)",
                width="small",
                options=STATUS_OPTIONS,
                required=True
            ),
            "집행URL": st.column_config.LinkColumn(
                "집행URL",
                help="집행 URL (클릭하면 링크로 이동)",
                width="medium",
                max_chars=None,
                validate="^https?://.*"
            ),
            "배정월": st.column_config.TextColumn(
                "배정월",
                width="small",
                help="배정 월",
                max_chars=None
            ),
            "브랜드": st.column_config.TextColumn(
                "브랜드",
                help="브랜드명",
                max_chars=None
            ),
                    "id": st.column_config.TextColumn(
            "id",
                help="인플루언서 ID",
                max_chars=None
            ),
            "이름": st.column_config.TextColumn(
                "이름",
                help="인플루언서 이름",
                max_chars=None
            ),
            "FLW": st.column_config.NumberColumn(
                "FLW",
                help="팔로워 수",
                format="%d",
                step=1
            ),
            "1회계약단가": st.column_config.NumberColumn(
                "1회계약단가",
                help="1회 계약 단가 (총액/전체계약수)",
                format="%d",
                step=1
            ),
            "2차활용": st.column_config.SelectboxColumn(
                "2차활용",
                help="2차활용 여부",
                options=["O", "X"],
                required=True
            ),
            "2차기간": st.column_config.TextColumn(
                "2차기간",
                help="2차활용 기간",
                max_chars=None
            ),
            "브랜드_계약수": st.column_config.NumberColumn(
                "브랜드_계약수",
                help="브랜드별 계약 수",
                format="%d",
                step=1
            ),
            "브랜드_잔여수": st.column_config.NumberColumn(
                "브랜드_잔여수",
                help="브랜드별 잔여 수 (계약수 - 실집행수)",
                format="%d",
                step=1
            ),


        }
    )

def handle_assignment_changes(edited_df, all_results_with_checkbox, df):
    """배정 변경사항 처리"""
    if edited_df is not None and not edited_df.empty:
        # URL 변경사항 처리
        handle_url_changes(edited_df, all_results_with_checkbox)
        
        # 상태 변경사항 처리
        handle_result_changes(edited_df, all_results_with_checkbox)

def handle_url_changes(edited_df, all_results_with_checkbox):
    """URL 변경사항 처리"""
    url_changes = []
    for idx, row in edited_df.iterrows():
        original_url = all_results_with_checkbox.loc[idx, '집행URL']
        new_url = row['집행URL']
        if original_url != new_url and pd.notna(new_url) and new_url != "":
            url_changes.append({
                'id': row['id'],
                '브랜드': row['브랜드'],
                '배정월': row['배정월'],
                '집행URL': new_url
            })
    
    if url_changes:
        update_assignment_urls(url_changes)
        create_success_container(f"✅ {len(url_changes)}개의 URL이 업데이트되었습니다!", "url_update_success")
        st.session_state.url_updated = True

def handle_result_changes(edited_df, all_results_with_checkbox):
    """상태 변경사항 처리"""
    changed_to_executed = []
    changed_to_assigned = []
    
    for idx, row in edited_df.iterrows():
        original_result = all_results_with_checkbox.loc[idx, '상태']
        new_result = row['상태']
        
        if original_result == '📋 배정완료' and new_result == '✅ 집행완료':
            changed_to_executed.append({
                'id': row['id'],
                '이름': row['이름'],
                '브랜드': row['브랜드'],
                '배정월': row['배정월']
            })
        elif original_result == '✅ 집행완료' and new_result == '📋 배정완료':
            changed_to_assigned.append({
                'id': row['id'],
                '이름': row['이름'],
                '브랜드': row['브랜드'],
                '배정월': row['배정월']
            })
    
    if changed_to_executed:
        update_execution_data(changed_to_executed, add=True)
        # 배정 데이터는 유지 (삭제하지 않음)
        create_success_container(f"✅ {len(changed_to_executed)}개의 배정이 실집행완료로 처리되었습니다!", "result_success")
        st.session_state.execution_updated = True
    
    if changed_to_assigned:
        update_execution_data(changed_to_assigned, add=False)
        create_success_container(f"✅ {len(changed_assigned)}개의 배정이 배정완료로 되돌려졌습니다!", "revert_success")
        st.session_state.assignment_reverted = True

def update_assignment_urls(url_changes):
    """배정 URL 업데이트"""
    if os.path.exists(ASSIGNMENT_FILE):
        assignment_history = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        if '집행URL' not in assignment_history.columns:
            assignment_history['집행URL'] = ""
    else:
        assignment_history = pd.DataFrame(columns=["브랜드", "id", "이름", "배정월", "집행URL"])
    
    for change in url_changes:
        mask = (
            (assignment_history['id'] == change['id']) &
            (assignment_history['브랜드'] == change['브랜드']) &
            (assignment_history['배정월'] == change['배정월'])
        )
        if mask.any():
            assignment_history.loc[mask, '집행URL'] = change['집행URL']
    
    # 클라우드에서는 GitHub 동기화, 로컬에서는 로컬 저장만
    if is_running_on_streamlit_cloud():
        save_with_auto_sync(assignment_history, ASSIGNMENT_FILE, "집행URL 업데이트")
    else:
        save_local_only(assignment_history, ASSIGNMENT_FILE)

def update_execution_data(changes, add=True):
    """실행 데이터 업데이트"""
    if os.path.exists(EXECUTION_FILE):
        execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
    else:
        execution_data = pd.DataFrame(columns=["id", "이름", "브랜드", "배정월", "실제집행수"])
    
    for change in changes:
        existing_mask = (
            (execution_data['id'] == change['id']) &
            (execution_data['브랜드'] == change['브랜드']) &
            (execution_data['배정월'] == change['배정월'])
        )
        
        if add:
            # 집행완료로 변경: 실행 데이터에 추가 또는 업데이트
            if existing_mask.any():
                execution_data.loc[existing_mask, '실제집행수'] = 1
            else:
                new_row = {**change, '실제집행수': 1}
                execution_data = pd.concat([execution_data, pd.DataFrame([new_row])], ignore_index=True)
        else:
            # 배정완료로 되돌리기: 실행 데이터에서만 제거 (배정 데이터는 유지)
            execution_data = execution_data[~existing_mask]
    
    # 클라우드에서는 GitHub 동기화, 로컬에서는 로컬 저장만
    if is_running_on_streamlit_cloud():
        save_with_auto_sync(execution_data, EXECUTION_FILE, "집행 데이터 업데이트")
    else:
        save_local_only(execution_data, EXECUTION_FILE)

def render_assignment_buttons(edited_df, df):
    """배정 버튼들 렌더링"""
    # 버튼 너비를 줄이기 위해 컬럼 비율 조정
    col1, col2, col3, col_spacer, col4 = st.columns([0.15, 0.15, 0.15, 0.1, 0.45])
    
    with col1:
        render_execution_complete_button(edited_df)
    
    with col2:
        render_delete_assignment_button(edited_df, df)
    
    with col3:
        render_reset_assignment_button(df)
    
    with col4:
        pass  # 빈 공간

def render_execution_complete_button(edited_df):
    """집행완료 버튼 렌더링"""
    if st.button("✅ 집행완료", type="secondary", use_container_width=True):
        selected_rows = edited_df[edited_df['선택'] == True]
        
        if not selected_rows.empty:
            changes = []
            for _, row in selected_rows.iterrows():
                changes.append({
                    'id': row['id'],
                    '이름': row['이름'],
                    '브랜드': row['브랜드'],
                    '배정월': row['배정월']
                })
            
            update_execution_data(changes, add=True)
            create_success_container(f"✅ {len(changes)}개의 배정이 실집행완료로 처리되었습니다!", "close_success")
            st.rerun()
        else:
            create_warning_container("⚠️ 실집행완료할 배정을 선택해주세요.", "close_warning")

def render_delete_assignment_button(edited_df, df):
    """배정 삭제 버튼 렌더링"""
    if st.button("❌ 배정 삭제", type="secondary", use_container_width=True):
        # 선택된 행 인덱스 사용
        selected_rows = st.session_state.get('selected_rows', [])
        
        if selected_rows and edited_df is not None and not edited_df.empty:
            execution_completed_selected = []
            deletable_rows = []
            
            for idx in selected_rows:
                if idx < len(edited_df):
                    row = edited_df.iloc[idx]
                if is_execution_completed(row):
                    execution_completed_selected.append(f"{row['이름']} ({row['브랜드']})")
                else:
                    deletable_rows.append(row)
            
            if execution_completed_selected:
                create_warning_container("집행완료 상태의 배정이 있어 삭제할 수 없습니다. 집행완료를 배정완료로 변경한 후 다시 시도해주세요.", "close_delete_warning")
            
            if deletable_rows:
                delete_assignments(deletable_rows)
                st.success(f"✅ {len(deletable_rows)}개의 배정이 삭제되었습니다!")
                # 사용자가 알림을 읽을 수 있도록 3초 대기
                time.sleep(3)
                st.rerun()
        else:
            st.warning("⚠️ 삭제할 배정을 선택해주세요.")

def render_reset_assignment_button(df):
    """배정초기화 버튼 렌더링"""
    # 초기화 상태 확인
    if 'reset_verification_done' not in st.session_state:
        st.session_state.reset_verification_done = False
    if 'reset_confirmation_shown' not in st.session_state:
        st.session_state.reset_confirmation_shown = False
    
    if st.button("🗑️ 배정초기화", type="secondary", use_container_width=True):
        st.session_state.reset_verification_done = True
        st.session_state.reset_confirmation_shown = False
        st.rerun()
    
    # 검증 상태 표시
    if st.session_state.reset_verification_done:
        # 현재 필터 상태 가져오기
        current_month_filter = st.session_state.get('tab1_month_filter', '')
        current_brand_filter = st.session_state.get('tab1_brand_filter', '')
        
        # execution_status.csv에서 집행완료 데이터 확인
        has_execution_completed = False
        
        if os.path.exists(EXECUTION_FILE):
            execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
            
            if not execution_data.empty and '배정월' in execution_data.columns and '실제집행수' in execution_data.columns:
                # 실제집행수가 0보다 큰 데이터만 필터링
                completed_data = execution_data[execution_data['실제집행수'] > 0]
                
                if current_month_filter and current_month_filter != "전체":
                    # 해당 월의 집행완료 데이터만 확인
                    month_completed = completed_data[completed_data['배정월'] == current_month_filter]
                    has_execution_completed = len(month_completed) > 0
                else:
                    # 전체 집행완료 데이터 확인
                    has_execution_completed = len(completed_data) > 0
        
        if has_execution_completed and not st.session_state.reset_confirmation_shown:
            # 경고 메시지와 함께 진행 옵션 제공
            if current_month_filter == "전체":
                st.warning("⚠️ 집행완료 상태의 배정이 있어 전체 초기화할 수 없습니다.")
            else:
                st.warning(f"⚠️ {current_month_filter}의 집행완료 상태의 배정이 있어 초기화할 수 없습니다.")
            st.info("💡 그래도 배정 초기화를 진행하시겠습니까?")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("❌ 취소", key="cancel_reset", use_container_width=True):
                    st.session_state.reset_verification_done = False
                    st.session_state.reset_confirmation_shown = False
            with col2:
                if st.button("✅ 예, 진행합니다", key="proceed_reset", use_container_width=True):
                    st.session_state.reset_confirmation_shown = True
                    # 전체 선택 상태 초기화
                    if 'select_all' in st.session_state:
                        st.session_state.select_all = False
                    
                    # 초기화 실행 (필터에 따라)
                    if current_month_filter == "전체":
                        reset_all_assignments()
                        st.success("✅ 전체 배정이 초기화되었습니다!")
                    else:
                        reset_assignments_for_month(current_month_filter)
                        st.success(f"✅ {current_month_filter} 배정이 초기화되었습니다!")
                    
                    # 사용자가 알림을 읽을 수 있도록 3초 대기
                    time.sleep(3)
                    
                    # 상태 초기화
                    st.session_state.reset_verification_done = False
                    st.session_state.reset_confirmation_shown = False
        elif not has_execution_completed:
            # 전체 선택 상태 초기화
            if 'select_all' in st.session_state:
                st.session_state.select_all = False
            
            # 초기화 실행 (필터에 따라)
            if current_month_filter == "전체":
                reset_all_assignments()
                st.success("✅ 전체 배정이 초기화되었습니다!")
            else:
                reset_assignments_for_month(current_month_filter)
                st.success(f"✅ {current_month_filter} 배정이 초기화되었습니다!")
            
            # 사용자가 알림을 읽을 수 있도록 3초 대기
            time.sleep(3)
            
            # 상태 초기화
            st.session_state.reset_verification_done = False
            st.session_state.reset_confirmation_shown = False

def reset_all_assignments():
    """전체 배정 초기화"""
    try:
        # assignment_history.csv 파일 삭제
        if os.path.exists(ASSIGNMENT_FILE):
            os.remove(ASSIGNMENT_FILE)
        
        # execution_status.csv 파일 삭제
        if os.path.exists(EXECUTION_FILE):
            os.remove(EXECUTION_FILE)
        
        # 성공 메시지
        st.success("✅ 전체 배정이 초기화되었습니다!")
        
    except Exception as e:
        st.error(f"❌ 전체 배정 초기화 중 오류 발생: {str(e)}")

def reset_assignments_for_month(month):
    """특정 월의 배정만 초기화"""
    try:
        # assignment_history.csv에서 해당 월 데이터 제거
        if os.path.exists(ASSIGNMENT_FILE):
            assignment_df = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
            if not assignment_df.empty:
                # 해당 월이 아닌 데이터만 유지
                filtered_df = assignment_df[assignment_df['배정월'] != month]
                if len(filtered_df) != len(assignment_df):
                    filtered_df.to_csv(ASSIGNMENT_FILE, index=False, encoding="utf-8")
        
        # execution_status.csv에서 해당 월 데이터 제거
        if os.path.exists(EXECUTION_FILE):
            execution_df = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
            if not execution_df.empty:
                # 해당 월이 아닌 데이터만 유지
                filtered_exec_df = execution_df[execution_df['배정월'] != month]
                if len(filtered_exec_df) != len(execution_df):
                    filtered_exec_df.to_csv(EXECUTION_FILE, index=False, encoding="utf-8")
        
        # 성공 메시지
        st.success(f"✅ {month} 배정이 초기화되었습니다!")
        
    except Exception as e:
        st.error(f"❌ {month} 배정 초기화 중 오류 발생: {str(e)}")

def is_execution_completed(row):
    """집행완료 상태인지 확인"""
    if os.path.exists(EXECUTION_FILE):
        execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
        # execution_data가 비어있거나 필요한 컬럼이 없으면 False 반환
        if execution_data.empty or 'id' not in execution_data.columns or '실제집행수' not in execution_data.columns:
            return False
        
        exec_mask = (
            (execution_data['id'] == row['id']) &
            (execution_data['브랜드'] == row['브랜드']) &
            (execution_data['배정월'] == row['배정월'])
        )
        return exec_mask.any() and execution_data.loc[exec_mask, '실제집행수'].iloc[0] > 0
    return False

def get_execution_completed_assignments_for_month(selected_month):
    """특정 월의 집행완료된 배정 목록 가져오기"""
    try:
        execution_completed_assignments = []
        
        print(f"DEBUG: 함수 호출 - 선택된 월: {selected_month}")
        
        # execution_data 파일 확인
        if not os.path.exists(EXECUTION_FILE):
            print(f"DEBUG: execution_status.csv 파일이 존재하지 않음")
            return execution_completed_assignments
        
        execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
        print(f"DEBUG: execution_data 로드 완료 - 행 수: {len(execution_data)}")
        
        # execution_data가 비어있거나 필요한 컬럼이 없으면 빈 리스트 반환
        if execution_data.empty:
            print(f"DEBUG: execution_data가 비어있음")
            return execution_completed_assignments
            
        if 'id' not in execution_data.columns or '실제집행수' not in execution_data.columns or '배정월' not in execution_data.columns:
            print(f"DEBUG: 필요한 컬럼이 없음 - 컬럼: {list(execution_data.columns)}")
            return execution_completed_assignments
        
        # 전체 집행완료 데이터 확인
        all_completed = execution_data[execution_data['실제집행수'] > 0]
        print(f"DEBUG: 전체 집행완료 데이터: {len(all_completed)}개")
        print(f"DEBUG: 전체 집행완료 데이터의 월: {all_completed['배정월'].unique()}")
        
        # 선택된 월의 집행완료 데이터만 필터링
        if selected_month:
            # 해당 월의 실제집행수가 0보다 큰 데이터만 선택
            month_executions = execution_data[
                (execution_data['배정월'] == selected_month) & 
                (execution_data['실제집행수'] > 0)
            ]
            print(f"DEBUG: {selected_month} 집행완료 데이터: {len(month_executions)}개")
        else:
            # 전체 월의 실제집행수가 0보다 큰 데이터만 선택
            month_executions = execution_data[execution_data['실제집행수'] > 0]
        
        # 집행완료된 배정 목록 생성
        for _, row in month_executions.iterrows():
            execution_completed_assignments.append(f"{row['이름']} ({row['브랜드']})")
    
        print(f"DEBUG: 최종 집행완료 배정 수: {len(execution_completed_assignments)}")
        return execution_completed_assignments
        
    except Exception as e:
        print(f"ERROR in get_execution_completed_assignments_for_month: {e}")
        return []

def delete_assignments(deletable_rows):
    """배정 삭제"""
    assignment_history = load_assignment_history()
    rows_to_remove = []
    
    for row in deletable_rows:
        mask = (
            (assignment_history['브랜드'] == row['브랜드']) &
            (assignment_history['id'] == row['id']) &
            (assignment_history['배정월'] == row['배정월'])
        )
        rows_to_remove.extend(assignment_history[mask].index.tolist())
    
    rows_to_remove = list(set(rows_to_remove))
    assignment_history = assignment_history.drop(rows_to_remove)
    # 클라우드에서는 GitHub 동기화, 로컬에서는 로컬 저장만
    if is_running_on_streamlit_cloud():
        save_with_auto_sync(assignment_history, ASSIGNMENT_FILE, "배정 삭제")
    else:
        save_local_only(assignment_history, ASSIGNMENT_FILE)

def reset_assignments():
    """배정 초기화"""
    # 현재 선택된 월을 정확히 가져오기
    current_month = st.session_state.get('tab1_month_filter', '')
    
    try:
        if current_month:
            # 선택된 월의 배정만 삭제
            assignment_history = load_assignment_history()
            if not assignment_history.empty:
                # 해당 월의 배정 제거
                assignment_history = assignment_history[assignment_history['배정월'] != current_month]
                # GitHub Actions로 자동 동기화 저장
                save_with_auto_sync(assignment_history, ASSIGNMENT_FILE, f"Reset assignments for {current_month}")
            
            # 선택된 월의 집행 데이터만 삭제
            if os.path.exists(EXECUTION_FILE):
                execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
                if not execution_data.empty:
                    execution_data = execution_data[execution_data['배정월'] != current_month]
                    # GitHub Actions로 자동 동기화 저장
                    save_with_auto_sync(execution_data, EXECUTION_FILE, f"Reset assignments for {current_month}")
            
            st.success(f"✅ {current_month} 배정이 초기화되었습니다!")
        else:
            # 월이 선택되지 않은 경우 전체 초기화
            if os.path.exists(ASSIGNMENT_FILE):
                os.remove(ASSIGNMENT_FILE)
            if os.path.exists(EXECUTION_FILE):
                os.remove(EXECUTION_FILE)
            st.success("✅ 모든 배정이 초기화되었습니다!")
            
    except Exception as e:
        st.error(f"❌ 배정 초기화 중 오류가 발생했습니다: {e}")

def render_excel_upload_section(df):
    """엑셀 업로드 섹션 렌더링"""
    st.markdown("---")
    st.subheader("📤 엑셀 업로드")
    st.markdown("💡 **다운로드한 엑셀 파일을 수정한 후 업로드하여 배정 및 실집행상태를 업데이트하세요**")
    
    # 업로드 모드 선택
    upload_mode = st.radio(
        "업로드 모드 선택",
        ["기존 데이터 업데이트", "전체 데이터 교체"],
        help="기존 데이터 업데이트: 기존 배정에 추가/수정, 전체 데이터 교체: 기존 데이터를 모두 삭제하고 새 데이터로 교체"
    )
    
    uploaded_file = st.file_uploader(
        "배정 및 실집행상태 엑셀 파일 업로드",
        type=['xlsx', 'xls'],
        help="수정한 엑셀 파일을 업로드하여 배정 및 실집행상태를 업데이트하세요"
    )
    
    if uploaded_file is not None:
        handle_excel_upload(uploaded_file, df, upload_mode)

def handle_excel_upload(uploaded_file, df, upload_mode):
    """엑셀 업로드 처리"""
    try:
        if uploaded_file.name.endswith('.xlsx'):
            uploaded_data = pd.read_excel(uploaded_file, engine='openpyxl')
        else:
            uploaded_data = pd.read_excel(uploaded_file, engine='xlrd')
        
        # 필수 컬럼만 검증 (id, 브랜드, 배정월, 상태 필수)
        required_columns = ['id', '브랜드', '배정월', '상태']
        missing_columns = [col for col in required_columns if col not in uploaded_data.columns]
        
        if missing_columns:
            st.error(f"❌ 필수 컬럼이 누락되었습니다: {', '.join(missing_columns)}")
        else:
            # 전체 데이터 교체 모드일 때 확인 다이얼로그 표시
            if upload_mode == "전체 데이터 교체":
                st.warning("⚠️ **전체 데이터 교체 모드**")
                st.markdown("**기존의 모든 배정 및 집행 데이터가 삭제되고 새로운 데이터로 완전히 교체됩니다.**")
                st.markdown("**이 작업은 되돌릴 수 없습니다.**")
                
                col1, col2, col3 = st.columns([1, 1, 1])
                with col1:
                    if st.button("✅ 전체 데이터 교체 실행", type="primary"):
                        process_uploaded_data(uploaded_data, df, upload_mode)
                with col2:
                    if st.button("❌ 취소"):
                        st.session_state.upload_cancelled = True
                with col3:
                    st.empty()  # 빈 공간
            else:
                # 기존 데이터 업데이트 모드는 바로 실행
                process_uploaded_data(uploaded_data, df, upload_mode)
            
    except Exception as e:
        st.error(f"❌ 파일 업로드 중 오류가 발생했습니다: {str(e)}")

def process_uploaded_data(uploaded_data, df, upload_mode):
    """업로드된 데이터 처리"""
    # 필수 컬럼 확인
    required_columns = ['id', '브랜드', '배정월', '상태']
    
    # 필수 컬럼이 있으면 처리 진행
    if all(col in uploaded_data.columns for col in required_columns):
        # 계약수 검증 및 기본 정보 자동 채우기
        valid_assignments = []
        invalid_assignments = []
        
        for idx, row in uploaded_data.iterrows():
            # id로 인플루언서 정보 찾기
            influencer_info = df[df['id'] == row['id']]
            if influencer_info.empty:
                invalid_assignments.append(f"ID '{row['id']}'를 찾을 수 없습니다.")
                continue
            
            # 브랜드 확인 (필수 컬럼)
            brand = row['브랜드']
            brand_qty_col = f"{brand.lower()}_qty"
            
            # 유효한 배정 데이터로 추가
            assignment_row = row.copy()
            
            # 기본 정보 자동 채우기
            assignment_row['이름'] = influencer_info.iloc[0]['name']
            assignment_row['FLW'] = influencer_info.iloc[0]['follower']
            assignment_row['1회계약단가'] = influencer_info.iloc[0]['unit_fee']
            assignment_row['2차활용'] = influencer_info.iloc[0]['sec_usage']
            assignment_row['2차기간'] = influencer_info.iloc[0]['sec_period']
            assignment_row['브랜드'] = brand
            
            # 브랜드_계약수 자동 채우기 (있으면 가져오고, 없으면 빈 값)
            if brand_qty_col in df.columns and brand_qty_col in influencer_info.columns:
                assignment_row['브랜드_계약수'] = influencer_info.iloc[0][brand_qty_col]
            else:
                assignment_row['브랜드_계약수'] = ""
            
            # 집행URL 컬럼이 없으면 빈 값으로 추가
            if '집행URL' not in assignment_row:
                assignment_row['집행URL'] = ''
            
            valid_assignments.append(assignment_row)
        
        # 오류가 있으면 표시하고 중단
        if invalid_assignments:
            st.error("❌ 다음 배정 데이터를 업로드할 수 없습니다:")
            for error in invalid_assignments:
                st.error(f"  • {error}")
            return
        
        # 유효한 배정 데이터만 처리
        if valid_assignments:
            assignment_update_data = pd.DataFrame(valid_assignments)
            update_assignment_history(assignment_update_data, df, upload_mode)
    
    # 실집행수 데이터 업데이트 (브랜드_실집행수 컬럼이 있는 경우에만)
    if '브랜드_실집행수' in uploaded_data.columns:
        execution_update_data = uploaded_data[uploaded_data['브랜드_실집행수'] > 0][['id', '브랜드', '배정월', '브랜드_실집행수']].copy()
        execution_update_data = execution_update_data.rename(columns={'브랜드_실집행수': '실제집행수'})
        execution_update_data = execution_update_data.merge(
                df[['id', 'name']].rename(columns={'id': 'id', 'name': '이름'}),
                on='id',
            how='left'
        )
        update_execution_history(execution_update_data, upload_mode)
    else:
        execution_update_data = pd.DataFrame()
    
    # 업데이트된 배정 데이터 수 계산
    existing_assignment_data = load_assignment_history()
    updated_count = len([row for row in assignment_update_data.iterrows() if 
                        any((existing_assignment_data['id'] == row[1]['id']) & 
                            (existing_assignment_data['브랜드'] == row[1]['브랜드']) & 
                            (existing_assignment_data['배정월'] == row[1]['배정월']))])
    new_count = len(assignment_update_data) - updated_count
    
    success_message = f"✅ "
    if new_count > 0:
        success_message += f"{new_count}개의 새로운 배정 데이터가 추가되었습니다. "
    if updated_count > 0:
        success_message += f"{updated_count}개의 기존 배정 데이터가 업데이트되었습니다. "
    if len(execution_update_data) > 0:
        success_message += f"{len(execution_update_data)}개의 실집행수 데이터가 업로드되었습니다."
    
    st.success(success_message)
    
    # 사용자가 알림을 읽을 수 있도록 3초 대기
    time.sleep(3)
    st.session_state.upload_completed = True

def update_assignment_history(assignment_update_data, df=None, upload_mode=None):
    """배정 이력 업데이트"""
    if os.path.exists(ASSIGNMENT_FILE):
        existing_assignment_data = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        if '집행URL' not in existing_assignment_data.columns:
            existing_assignment_data['집행URL'] = ""
    else:
        existing_assignment_data = pd.DataFrame(columns=["브랜드", "id", "이름", "배정월", "집행URL"])
    
    # 전체 데이터 교체 모드인 경우 기존 데이터를 완전히 교체
    if upload_mode == "전체 데이터 교체":
        combined_assignment_data = assignment_update_data.copy()
    else:
        # 기존 데이터 업데이트 모드
        # 업데이트된 데이터를 기존 데이터와 병합
        updated_data = []
        new_data = []
        
        for _, new_row in assignment_update_data.iterrows():
            # 기존 데이터에서 동일한 id, 브랜드, 배정월 조합 찾기
            existing_mask = (
                (existing_assignment_data['id'] == new_row['id']) &
                (existing_assignment_data['브랜드'] == new_row['브랜드']) &
                (existing_assignment_data['배정월'] == new_row['배정월'])
            )
            
            if existing_mask.any():
                # 기존 데이터가 있으면 업데이트 (상태, 집행URL 등만 변경)
                existing_row = existing_assignment_data[existing_mask].iloc[0].copy()
                
                # 업데이트 가능한 필드들만 변경
                updateable_fields = ['상태', '집행URL', '이름', 'FLW', '1회계약단가', '2차활용', '2차기간', '브랜드_계약수']
                for field in updateable_fields:
                    if field in new_row and field in existing_row:
                        existing_row[field] = new_row[field]
                
                updated_data.append(existing_row)
            else:
                # 새로운 데이터는 추가
                new_data.append(new_row)
        
        # 기존 데이터에서 업데이트되지 않은 데이터 유지
        updated_ids = [(row['id'], row['브랜드'], row['배정월']) for row in updated_data]
        remaining_data = existing_assignment_data[
            ~existing_assignment_data.apply(
                lambda row: (row['id'], row['브랜드'], row['배정월']) in updated_ids, axis=1
            )
        ]
        
        # 모든 데이터 병합
        combined_assignment_data = pd.concat([remaining_data, pd.DataFrame(updated_data), pd.DataFrame(new_data)], ignore_index=True)
    
    # 클라우드에서는 GitHub 동기화, 로컬에서는 로컬 저장만
    if is_running_on_streamlit_cloud():
        save_with_auto_sync(combined_assignment_data, ASSIGNMENT_FILE, "Update assignment history from Excel upload")
    else:
        save_local_only(combined_assignment_data, ASSIGNMENT_FILE)

def update_execution_history(execution_update_data, upload_mode=None):
    """실행 이력 업데이트"""
    if os.path.exists(EXECUTION_FILE):
        existing_execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
    else:
        existing_execution_data = pd.DataFrame(columns=["id", "이름", "브랜드", "배정월", "실제집행수"])
    
    # 전체 데이터 교체 모드인 경우 기존 데이터를 완전히 교체
    if upload_mode == "전체 데이터 교체":
        combined_execution_data = execution_update_data.copy()
    else:
        # 기존 데이터 업데이트 모드
        combined_execution_data = pd.concat([existing_execution_data, execution_update_data], ignore_index=True)
        combined_execution_data = combined_execution_data.drop_duplicates(subset=['id', '브랜드', '배정월'], keep='last')
    
    # 클라우드에서는 GitHub 동기화, 로컬에서는 로컬 저장만
    if is_running_on_streamlit_cloud():
        save_with_auto_sync(combined_execution_data, EXECUTION_FILE, "Update execution history from Excel upload")
    else:
        save_local_only(combined_execution_data, EXECUTION_FILE)

def render_influencer_tab(df):
    """인플루언서별 탭 렌더링"""
    st.subheader("👥 인플루언서별 배정 현황")
    
    # 필터 섹션을 컨테이너로 감싸서 일관된 공간 확보
    with st.container():
        # 시즌 필터 - contract_sesn 데이터에서 시즌 추출
        season_options = get_season_options(df)
        selected_season_filter = st.selectbox("🏆 시즌", season_options, index=0, key="tab2_season_filter")
    
    # 브랜드 필터
        selected_brand_filter = st.selectbox("🏷️ 브랜드", BRAND_OPTIONS, index=0, key="tab2_brand_filter")
    
    # 테이블 섹션을 컨테이너로 감싸서 일관된 공간 확보
    with st.container():
    # 인플루언서 요약 데이터 준비
        influencer_summary = prepare_influencer_summary(df, selected_brand_filter, selected_season_filter)
    
    if not influencer_summary.empty:
            render_influencer_table(influencer_summary, selected_brand_filter, selected_season_filter, influencer_count=len(influencer_summary))
    else:
        st.info("인플루언서 데이터가 없습니다.")
    


def prepare_influencer_summary(df, selected_brand_filter, selected_season_filter):
    """인플루언서 요약 데이터 준비"""
    influencer_summary = df[["id", "name", "follower", "unit_fee", "sec_usage", "sec_period"]].copy()
    
    # 전체 계약수 계산
    qty_cols = [f"{brand.lower()}_qty" for brand in BRANDS]
    influencer_summary["전체_계약수"] = df.loc[influencer_summary.index, qty_cols].sum(axis=1)
    
    # 시즌 필터 적용
    # 배정월 필터와 동일한 시즌 로직 적용
    if selected_season_filter == "25FW":
        # 25FW 시즌 (9월~12월, 1월~2월) 데이터만 필터링
        influencer_summary = filter_by_season(influencer_summary, df, ["9월", "10월", "11월", "12월", "1월", "2월"])
    elif selected_season_filter == "26SS":
        # 26SS 시즌 (3월~8월) 데이터만 필터링
        influencer_summary = filter_by_season(influencer_summary, df, ["3월", "4월", "5월", "6월", "7월", "8월"])
    
    # 브랜드 필터 적용
    if selected_brand_filter != "전체":
        qty_col = f"{selected_brand_filter.lower()}_qty"
        if qty_col in df.columns:
            brand_filter_mask = df[qty_col] > 0
            influencer_summary = influencer_summary[brand_filter_mask]
    # 브랜드 필터가 "전체"일 때는 모든 인플루언서 표시 (필터링하지 않음)
    
    # 브랜드별 상세 정보 추가
    add_brand_details(influencer_summary, df, selected_brand_filter)
    
    # 번호 컬럼 추가
    influencer_summary = influencer_summary.reset_index(drop=True)
    influencer_summary.insert(0, '번호', range(1, len(influencer_summary) + 1))
    
    # 컬럼명 변경
    influencer_summary = influencer_summary.rename(columns={
        "id": "id", "name": "이름", "follower": "FLW", "unit_fee": "1회계약단가", "sec_usage": "2차활용", "sec_period": "2차기간"
    })
    
    # 전체 필터에서도 전체_계약수, 전체_집행수, 전체_잔여수 컬럼 유지 (2차활용 오른쪽에 위치)
    # 전체_계약수, 전체_집행수, 전체_잔여수 컬럼을 2차활용 다음 위치로 이동
    if "전체_계약수" in influencer_summary.columns:
        # 2차활용 컬럼 다음 위치에 전체_계약수, 전체_집행수, 전체_잔여수 이동
        cols = list(influencer_summary.columns)
        if "2차활용" in cols and "전체_계약수" in cols:
            # 2차활용 위치 찾기
            sec_usage_idx = cols.index("2차활용")
            
            # 전체 관련 컬럼들 제거
            cols_to_remove = ["전체_계약수"]
            if "전체_집행수" in cols:
                cols_to_remove.append("전체_집행수")
            if "전체_잔여수" in cols:
                cols_to_remove.append("전체_잔여수")
            
            for col in cols_to_remove:
                if col in cols:
                    cols.remove(col)
            
            # 2차기간 컬럼이 이미 존재하므로 제거 후 올바른 위치에 재삽입
            if "2차기간" in cols:
                cols.remove("2차기간")
            
            # 2차활용 다음 위치에 2차기간, 전체_계약수, 전체_집행수, 전체_잔여수 순서대로 삽입
            cols.insert(sec_usage_idx + 1, "2차기간")
            cols.insert(sec_usage_idx + 2, "전체_계약수")
            if "전체_집행수" in influencer_summary.columns:
                cols.insert(sec_usage_idx + 3, "전체_집행수")
            if "전체_잔여수" in influencer_summary.columns:
                cols.insert(sec_usage_idx + 4, "전체_잔여수")
            
            influencer_summary = influencer_summary[cols]
    
    # 월별 컬럼 추가 (마지막에 실행)
    add_monthly_columns(influencer_summary, df, selected_brand_filter)
    
    return influencer_summary

def add_brand_details(influencer_summary, df, selected_brand_filter):
    """브랜드별 상세 정보 추가"""
    if selected_brand_filter != "전체":
        selected_brand = selected_brand_filter
        qty_col = f"{selected_brand.lower()}_qty"
        
        if qty_col in df.columns:
            influencer_summary[f"{selected_brand}_계약수"] = df.loc[influencer_summary.index, qty_col]
        else:
            influencer_summary[f"{selected_brand}_계약수"] = 0
        
        # 🚫 브랜드 필터 선택 시 해당 브랜드의 집행수/잔여수 컬럼은 표시하지 않음
        # (화면과 엑셀에서 모두 제거)
        
    else:
        # 전체 선택 시 모든 브랜드 계약수 표시
        for brand in BRANDS:
            qty_col = f"{brand.lower()}_qty"
            if qty_col in df.columns:
                influencer_summary[f"{brand}_계약수"] = df.loc[influencer_summary.index, qty_col]
            else:
                influencer_summary[f"{brand}_계약수"] = 0
            
        # 🚫 전체 선택 시에도 개별 브랜드의 집행수/잔여수 컬럼은 표시하지 않음
        # 전체_집행수와 전체_잔여수만 계산하여 표시
        if os.path.exists(EXECUTION_FILE):
            execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
            if not execution_data.empty and '실제집행수' in execution_data.columns:
                # 모든 브랜드의 집행완료 데이터 필터링
                all_executions = execution_data[execution_data['실제집행수'] > 0]
                
                # 인플루언서별 전체 집행수 계산
                id_column = 'id' if 'id' in all_executions.columns else 'id'
                total_executed = all_executions.groupby(id_column)['실제집행수'].sum()
                influencer_summary['전체_집행수'] = influencer_summary['id'].map(total_executed).fillna(0).astype(int)
                
                # 전체 배정완료 데이터 계산
                if os.path.exists(ASSIGNMENT_FILE):
                    assignment_data = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
                    if not assignment_data.empty:
                        # 모든 브랜드의 배정완료 데이터 필터링
                        all_assignments = assignment_data[assignment_data['상태'] == '배정완료']
                        
                        # 인플루언서별 전체 배정수 계산
                        total_assigned = all_assignments.groupby('id').size()
                        influencer_summary['전체_배정수'] = influencer_summary['id'].map(total_assigned).fillna(0).astype(int)
                    else:
                        influencer_summary['전체_배정수'] = 0
                else:
                    influencer_summary['전체_배정수'] = 0
                
                # 전체 잔여수 = 전체 계약수 - (전체 집행수 + 전체 배정수)
                influencer_summary['전체_잔여수'] = influencer_summary['전체_계약수'] - (influencer_summary['전체_집행수'] + influencer_summary['전체_배정수'])
            else:
                influencer_summary['전체_집행수'] = 0
                influencer_summary['전체_잔여수'] = influencer_summary['전체_계약수']
        else:
            influencer_summary['전체_집행수'] = 0
            influencer_summary['전체_잔여수'] = influencer_summary['전체_계약수']

def filter_by_season(influencer_summary, df, target_months):
    """시즌별 필터링"""
    # 25FW 시즌의 경우, 25FW 계약이 있는 인플루언서만 필터링
    if target_months == ["9월", "10월", "11월", "12월", "1월", "2월"]:  # 25FW
        # contract_sesn이 25FW인 인플루언서만 필터링
        season_filter_mask = df['contract_sesn'] == '25FW'
        season_influencer_ids = df[season_filter_mask]['id'].unique()
        filtered_summary = influencer_summary[influencer_summary['id'].isin(season_influencer_ids)]
        return filtered_summary
    elif target_months == ["3월", "4월", "5월", "6월", "7월", "8월"]:  # 26SS
        # contract_sesn이 26SS인 인플루언서만 필터링
        season_filter_mask = df['contract_sesn'] == '26SS'
        season_influencer_ids = df[season_filter_mask]['id'].unique()
        filtered_summary = influencer_summary[influencer_summary['id'].isin(season_influencer_ids)]
        return filtered_summary
    
    return influencer_summary

def add_monthly_columns(influencer_summary, df, selected_brand_filter):
    """월별 컬럼 추가"""
    months = ["9월", "10월", "11월", "12월", "1월", "2월"]
    for month in months:
        influencer_summary[month] = ""
    
    # 1. 집행완료된 배정 표시 (괄호 없이)
    if os.path.exists(EXECUTION_FILE):
        execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
        if not execution_data.empty and '실제집행수' in execution_data.columns:
            # 실제집행수가 0보다 큰 완료된 배정만 필터링
            completed_executions = execution_data[execution_data['실제집행수'] > 0]
            
            # 브랜드 필터 적용: 특정 브랜드가 선택된 경우 해당 브랜드의 집행만 표시
            if selected_brand_filter != "전체":
                completed_executions = completed_executions[completed_executions['브랜드'] == selected_brand_filter]
            
            # 인플루언서별, 월별로 브랜드 집계
            for _, row in influencer_summary.iterrows():
                influencer_id = row["id"]
                for month in months:
                    # 해당 인플루언서의 해당 월 집행 내역
                    month_executions = completed_executions[
                        (completed_executions['id'] == influencer_id) & 
                        (completed_executions['배정월'] == month)
                    ]
                    
                    if not month_executions.empty:
                        # 브랜드 필터 선택 시 상태값 표시, 전체 선택 시 브랜드명 표시
                        if selected_brand_filter != "전체":
                            # 특정 브랜드 필터 선택 시: 상태값 표시
                            influencer_summary.loc[influencer_summary['id'] == influencer_id, month] = "집행완료"
                        else:
                            # 전체 선택 시: 브랜드명 표시
                            brands = month_executions['브랜드'].unique()
                            brand_order = ["MLB", "DX", "DV", "ST"]
                            sorted_brands = [brand for brand in brand_order if brand in brands]
                            influencer_summary.loc[influencer_summary['id'] == influencer_id, month] = ", ".join(sorted_brands)
    
    # 2. 배정완료 상태인 배정 표시 (괄호로 감싸서)
    if os.path.exists(ASSIGNMENT_FILE):
        assignment_data = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        if not assignment_data.empty and '상태' in assignment_data.columns:
            # 배정완료 상태인 배정만 필터링
            completed_assignments = assignment_data[assignment_data['상태'] == '📋 배정완료']
            
            # 브랜드 필터 적용: 특정 브랜드가 선택된 경우 해당 브랜드의 배정만 표시
            if selected_brand_filter != "전체":
                completed_assignments = completed_assignments[completed_assignments['브랜드'] == selected_brand_filter]
            
            # 인플루언서별, 월별로 배정 상태 추가
            for _, row in influencer_summary.iterrows():
                influencer_id = row["id"]
                for month in months:
                    # 해당 인플루언서의 해당 월 배정 내역
                    month_assignments = completed_assignments[
                        (completed_assignments['id'] == influencer_id) & 
                        (completed_assignments['배정월'] == month)
                    ]
                    
                    if not month_assignments.empty:
                        # 브랜드별로 고정 순서로 표시 (MLB,DX,DV,ST)
                        brands = month_assignments['브랜드'].unique()
                        brand_order = ["MLB", "DX", "DV", "ST"]
                        sorted_brands = [brand for brand in brand_order if brand in brands]
                        
                        # 기존 집행완료 데이터가 있으면 추가, 없으면 새로 설정
                        current_value = influencer_summary.loc[influencer_summary['id'] == influencer_id, month].iloc[0]
                        if current_value and current_value.strip():
                            # 브랜드 필터 선택 시 상태값 표시, 전체 선택 시 브랜드명 표시
                            if selected_brand_filter != "전체":
                                # 특정 브랜드 필터 선택 시: 상태값 표시
                                if current_value == "집행완료":
                                    influencer_summary.loc[influencer_summary['id'] == influencer_id, month] = "집행완료, 배정완료"
                                elif "집행완료" in current_value:
                                    # 이미 집행완료가 포함된 경우 배정완료 추가
                                    influencer_summary.loc[influencer_summary['id'] == influencer_id, month] = current_value + ", 배정완료"
                                else:
                                    influencer_summary.loc[influencer_summary['id'] == influencer_id, month] = "배정완료"
                            else:
                                # 전체 선택 시: 브랜드명 표시 (괄호로 감싸서)
                                assignment_brands = [f"({brand})" for brand in sorted_brands]
                                influencer_summary.loc[influencer_summary['id'] == influencer_id, month] = current_value + ", " + ", ".join(assignment_brands)
                        else:
                            # 기존 값이 없으면 배정완료 상태만 표시
                            if selected_brand_filter != "전체":
                                # 특정 브랜드 필터 선택 시: 상태값 표시
                                influencer_summary.loc[influencer_summary['id'] == influencer_id, month] = "배정완료"
                            else:
                                # 전체 선택 시: 브랜드명 표시 (괄호로 감싸서)
                                assignment_brands = [f"({brand})" for brand in sorted_brands]
                                influencer_summary.loc[influencer_summary['id'] == influencer_id, month] = ", ".join(assignment_brands)


def render_influencer_table(influencer_summary, selected_brand_filter, selected_season_filter, influencer_count=None):
    """인플루언서 테이블 렌더링"""
    # 브랜드 하이라이트 CSS 추가 (selectbox에 영향 주지 않도록 수정)
    if selected_brand_filter != "전체":
        st.markdown(f"""
        <style>
        /* 테이블 셀에만 하이라이트 적용 (selectbox 제외) */
        .stDataFrame [data-testid="stDataFrameCell"]:has-text("{selected_brand_filter}") {{
            background-color: #e3f2fd !important;
            color: #1976d2 !important;
            font-weight: bold !important;
        }}
        
        /* selectbox 드롭다운 위치 보호 */
        .stSelectbox, .stSelectbox * {{
            position: relative !important;
            z-index: auto !important;
        }}
        
        /* selectbox 옵션 리스트 위치 고정 */
        .stSelectbox ul, .stSelectbox li {{
            position: relative !important;
            z-index: 1000 !important;
        }}
        </style>
        """, unsafe_allow_html=True)
    else:
        # 전체 필터일 때는 하이라이트 없음
        st.markdown("""
        <style>
        /* selectbox 드롭다운 위치 보호 */
        .stSelectbox, .stSelectbox * {
            position: relative !important;
            z-index: auto !important;
        }
        
        /* selectbox 옵션 리스트 위치 고정 */
        .stSelectbox ul, .stSelectbox li {
            position: relative !important;
            z-index: 1000 !important;
        }
        </style>
        """, unsafe_allow_html=True)
    
    # 인플루언서 수를 테이블 바로 위에 표시
    if influencer_count is not None:
        st.markdown(f"📊 인플루언서수 : {influencer_count}개")
    
    # 편집 가능한 데이터프레임으로 표시 (고정 너비로 일관된 레이아웃)
    with st.container():
        edited_influencer_df = st.data_editor(
        influencer_summary,
        use_container_width=True,
        height=600,
        hide_index=True,
        key="influencer_data_editor",
        column_config=get_influencer_column_config()
    )
    
    # 변경사항 처리
    handle_influencer_changes(edited_influencer_df)
    
    # 엑셀 다운로드와 배정초기화 버튼을 "배정 및 집행상태" 탭과 동일한 스타일로 배치
    col1, col2, col3, col_spacer, col4 = st.columns([0.15, 0.15, 0.15, 0.1, 0.45])
    
    with col1:
        try:
            # 멀티시트 Excel 생성 시도
            excel_data = create_multi_sheet_excel(influencer_summary, selected_brand_filter, selected_season_filter)
            st.download_button(
                "📥 엑셀 다운로드",
                excel_data,
                file_name="influencer_summary_multi_sheet.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="influencer_excel_download_button",
                use_container_width=True,
                type="secondary"  # primary → secondary로 변경하여 하얀색 배경
            )
        except Exception as e:
            # 멀티시트 생성 실패 시 기본 Excel 생성
            st.download_button(
                "📥 엑셀 다운로드 (기본)",
                to_excel_bytes(influencer_summary),
                file_name="influencer_summary.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="influencer_excel_download_button_fallback",
                use_container_width=True,
                type="secondary"
            )
    
    with col2:
        # 배정초기화 버튼
        if st.button("🗑️ 배정초기화", type="secondary", use_container_width=True, key="influencer_reset_button"):
            st.session_state.influencer_reset_verification_done = True
            st.session_state.influencer_reset_confirmation_shown = False
    
    with col3:
        pass  # 빈 공간
    
    with col4:
        pass  # 빈 공간
    
    # 배정초기화 검증 로직
    if st.session_state.get('influencer_reset_verification_done', False):
        # execution_status.csv에서 집행완료 데이터 확인
        has_execution_completed = False
        
        if os.path.exists(EXECUTION_FILE):
            execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
            
            if not execution_data.empty and '실제집행수' in execution_data.columns:
                # 실제집행수가 0보다 큰 데이터만 필터링
                completed_data = execution_data[execution_data['실제집행수'] > 0]
                has_execution_completed = len(completed_data) > 0
        
        if has_execution_completed and not st.session_state.get('influencer_reset_confirmation_shown', False):
            # 경고 메시지와 함께 진행 옵션 제공
            st.warning("⚠️ 집행완료 상태의 배정이 있어 초기화할 수 없습니다.")
            st.info("💡 그래도 배정 초기화를 진행하시겠습니까?")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("❌ 취소", key="influencer_cancel_reset", use_container_width=True):
                    st.session_state.influencer_reset_verification_done = False
                    st.session_state.influencer_reset_confirmation_shown = False
            with col2:
                if st.button("✅ 예, 진행합니다", key="influencer_proceed_reset", use_container_width=True):
                    st.session_state.influencer_reset_confirmation_shown = True
                    
                    # 전체 배정 초기화 실행
                    reset_all_assignments()
                    
                    # 성공 메시지 표시
                    st.success("✅ 전체 배정이 초기화되었습니다!")
                    
                    # 사용자가 알림을 읽을 수 있도록 3초 대기
                    time.sleep(3)
                    
                    # 상태 초기화
                    st.session_state.influencer_reset_verification_done = False
                    st.session_state.influencer_reset_confirmation_shown = False
        elif not has_execution_completed:
            # 전체 배정 초기화 실행
            reset_all_assignments()
            st.success("✅ 전체 배정이 초기화되었습니다!")
            
            # 사용자가 알림을 읽을 수 있도록 3초 대기
            time.sleep(3)
            
            # 상태 초기화
            st.session_state.influencer_reset_verification_done = False
            st.session_state.influencer_reset_confirmation_shown = False

def get_influencer_column_config():
    """인플루언서 컬럼 설정"""
    return {
        "번호": st.column_config.NumberColumn(
            "번호",
            help="순서 번호",
            format="%d"
        ),
        "id": st.column_config.TextColumn(
            "id",
            help="인플루언서 ID",
            max_chars=None
        ),
        "이름": st.column_config.TextColumn(
            "이름",
            help="인플루언서 이름",
            max_chars=None
        ),
        "FLW": st.column_config.NumberColumn(
            "FLW",
            help="팔로워 수",
            format="%d",
            step=1
        ),
        "1회계약단가": st.column_config.NumberColumn(
            "1회계약단가",
            help="1회 계약 단가 (총액/전체계약수)",
            format="%d",
            step=1
        ),
        "2차활용": st.column_config.SelectboxColumn(
            "2차활용",
            help="2차활용 여부",
            options=["O", "X"],
            required=True
        ),
        "2차기간": st.column_config.TextColumn(
            "2차기간",
            help="2차활용 기간",
            max_chars=None
        ),
        "전체_계약수": st.column_config.NumberColumn(
            "전체_계약수",
            help="전체 계약 수",
            format="%d",
            step=1
        ),
        "전체_집행수": st.column_config.NumberColumn(
            "전체_집행수",
            help="전체 집행 수",
            format="%d",
            step=1
        ),
        "전체_잔여수": st.column_config.NumberColumn(
            "전체_잔여수",
            help="전체 잔여 수 (전체계약수 - 전체집행수)",
            format="%d",
            step=1
        ),
        "MLB_계약수": st.column_config.NumberColumn(
            "MLB_계약수",
            help="MLB 계약 수",
            format="%d",
            step=1
        ),
        "DX_계약수": st.column_config.NumberColumn(
            "DX_계약수",
            help="DX 계약 수",
            format="%d",
            step=1
        ),
        "DV_계약수": st.column_config.NumberColumn(
            "DV_계약수",
            help="DV 계약 수",
            format="%d",
            step=1
        ),
        "ST_계약수": st.column_config.NumberColumn(
            "ST_계약수",
            help="ST 계약 수",
            format="%d",
            step=1
        ),
        "MLB_집행수": st.column_config.NumberColumn(
            "MLB_집행수",
            help="MLB 집행 수",
            format="%d",
            step=1
        ),
        "MLB_잔여수": st.column_config.NumberColumn(
            "MLB_잔여수",
            help="MLB 잔여 수 (계약수 - 집행수)",
            format="%d",
            step=1
        ),
        "DX_집행수": st.column_config.NumberColumn(
            "DX_집행수",
            help="DX 집행 수",
            format="%d",
            step=1
        ),
        "DX_잔여수": st.column_config.NumberColumn(
            "DX_잔여수",
            help="DX 잔여 수 (계약수 - 집행수)",
            format="%d",
            step=1
        ),
        "DV_집행수": st.column_config.NumberColumn(
            "DV_집행수",
            help="DV 집행 수",
            format="%d",
            step=1
        ),
        "DV_잔여수": st.column_config.NumberColumn(
            "DV_잔여수",
            help="DV 잔여 수 (계약수 - 집행수)",
            format="%d",
            step=1
        ),
        "ST_집행수": st.column_config.NumberColumn(
            "ST_집행수",
            help="ST 집행 수",
            format="%d",
            step=1
        ),
        "ST_잔여수": st.column_config.NumberColumn(
            "ST_잔여수",
            help="ST 잔여 수 (계약수 - 집행수)",
            format="%d",
            step=1
        ),
        "잔여횟수_MLB": st.column_config.NumberColumn(
            "잔여횟수_MLB",
            help="MLB 잔여 횟수",
            format="%d",
            step=1
        ),
        "잔여횟수_DX": st.column_config.NumberColumn(
            "잔여횟수_DX",
            help="DX 잔여 횟수",
            format="%d",
            step=1
        ),
        "잔여횟수_DV": st.column_config.NumberColumn(
            "잔여횟수_DV",
            help="DV 잔여 횟수",
            format="%d",
            step=1
        ),
        "잔여횟수_ST": st.column_config.NumberColumn(
            "잔여횟수_ST",
            help="ST 잔여 횟수",
            format="%d",
            step=1
        ),
        "9월": st.column_config.TextColumn(
            "9월",
            help="9월 배정월 배정 브랜드",
            max_chars=None
        ),
        "10월": st.column_config.TextColumn(
            "10월",
            help="10월 배정월 배정 브랜드",
            max_chars=None
        ),
        "11월": st.column_config.TextColumn(
            "11월",
            help="11월 배정월 배정 브랜드",
            max_chars=None
        ),
        "12월": st.column_config.TextColumn(
            "12월",
            help="12월 배정월 배정 브랜드",
            max_chars=None
        ),
        "1월": st.column_config.TextColumn(
            "1월",
            help="1월 배정월 배정 브랜드",
            max_chars=None
        ),
        "2월": st.column_config.TextColumn(
            "2월",
            help="2월 배정월 배정 브랜드",
            max_chars=None
        )
    }

def handle_influencer_changes(edited_influencer_df):
    """인플루언서 변경사항 처리"""
    if edited_influencer_df is not None and not edited_influencer_df.empty:
        assignment_history = load_assignment_history()
        months = ["9월", "10월", "11월", "12월", "1월", "2월"]
        new_assignments = []
        updated_assignments = []
        
        for _, row in edited_influencer_df.iterrows():
            if pd.notna(row['id']) and row['id'] != "":
                for month in months:
                    new_value = row[month]
                    if new_value and new_value != "":
                        # 쉼표가 포함된 브랜드 값은 표시용이므로 실제 배정 데이터에 저장하지 않음
                        if isinstance(new_value, str) and ',' in new_value:
                            # 복합 브랜드 값은 건너뛰기 (표시용이므로 실제 배정에 영향 없음)
                            continue
                        else:
                            # 단일 브랜드만 처리
                            existing_mask = (
                                (assignment_history['id'] == row['id']) &
                                (assignment_history['브랜드'] == new_value) &
                            (assignment_history['배정월'] == month)
                        )
                        
                        if not existing_mask.any():
                            new_assignments.append({
                                '브랜드': new_value,
                                    'id': row['id'],
                                    '이름': row['이름'],
                                    '배정월': month
                                })
        
        if new_assignments or updated_assignments:
            # GitHub Actions로 자동 동기화 저장
            save_with_auto_sync(assignment_history, ASSIGNMENT_FILE, "Update influencer assignments")
            st.session_state.assignments_updated = True

# =============================================================================
# 메인 앱
# =============================================================================

def get_season_options(df):
    """배정월 필터와 동일한 시즌 옵션 반환"""
    # 배정월 필터에서 사용하는 것과 동일한 시즌 옵션
    return ["25FW", "26SS"]

def get_month_options_for_season(season):
    """시즌에 따른 월 옵션 반환 (인플루언서별 탭용)"""
    return FW_MONTHS if season == "25FW" else SS_MONTHS

def get_influencer_info(influencer_id):
    """인플루언서 정보 가져오기"""
    df = load_influencer_data()
    if df is not None:
        influencer_data = df[df['id'] == influencer_id]
        if not influencer_data.empty:
            return influencer_data.iloc[0].to_dict()
    return None

def render_monthly_targets_tab(df):
    """배정수량관리 탭 렌더링"""
    st.header("🎯 월별 배정수량 관리")
    
    # 월별 배정수량 데이터 로드 또는 기본 데이터 생성
    if os.path.exists(MONTHLY_TARGETS_FILE):
        try:
            targets_df = pd.read_csv(MONTHLY_TARGETS_FILE)
            
            # 컬럼명 확인 및 수정
            if 'month' in targets_df.columns and 'brand' in targets_df.columns and 'target_quantity' in targets_df.columns:
                # 25FW 시즌 데이터만 필터링
                fw_df = targets_df[targets_df['season'] == '25FW']
                
                # 브랜드별로 피벗 테이블 생성 (브랜드가 열, 월이 행)
                pivot_df = fw_df.pivot(index='month', columns='brand', values='target_quantity').fillna(0)
                
                # 월 순서 정렬 (9월 → 10월 → 11월 → 12월 → 1월 → 2월)
                month_order = [9, 10, 11, 12, 1, 2]
                existing_months = [month for month in month_order if month in pivot_df.index]
                pivot_df = pivot_df.loc[existing_months]
                
                # 월 인덱스명을 한국어로 변경
                pivot_df.index = [MONTH_NAMES.get(month, f"{month}월") for month in pivot_df.index]
                pivot_df.index.name = "배정월"
                
                # 존재하는 브랜드만 처리 (빈값이어도 상관없음)
                available_brands = [brand for brand in BRANDS if brand in pivot_df.columns]
                if not available_brands:
                    st.warning("⚠️ 사용 가능한 브랜드가 없습니다.")
                    return
                
                # 존재하는 브랜드만 선택하여 피벗 테이블 구성
                pivot_df = pivot_df[available_brands]
                
                # 총 배정요청수량과 브랜드별 요청수량을 테이블 위에 표시
                if not pivot_df.empty:
                    total_requested = pivot_df.sum().sum()
                    brand_totals = pivot_df.sum()
                    
                    # 브랜드별 요청수량을 괄호 안에 간단하게 표시
                    brand_summary = ", ".join([f"{brand}: {brand_totals[brand]:,}건" for brand in available_brands if brand in brand_totals])
                    st.markdown(f"**📊 총 배정요청수량: {total_requested:,}건** ({brand_summary})")
                
                # 편집 가능한 데이터프레임
                edited_df = st.data_editor(
                    pivot_df,
                    use_container_width=True,
                    key="monthly_targets_editor",
                    hide_index=False,
                    column_config={
                        col: st.column_config.NumberColumn(col, min_value=0) 
                        for col in pivot_df.columns
                    }
                )
                
                # 버튼들
                col1, col2, col3, col_spacer, col4 = st.columns([0.15, 0.15, 0.1, 0.1, 0.5])
                
                with col1:
                    if st.button("💾 배정수량 저장", type="secondary", use_container_width=True):
                        try:
                            # 편집된 데이터를 원본 형식으로 변환하여 저장
                            # 피벗 테이블 → 원본 형식 (month, brand, target_quantity)
                            save_data = []
                            for month_idx, month_name in enumerate(edited_df.index):
                                for brand in edited_df.columns:
                                    value = edited_df.loc[month_name, brand]
                                    # 0값도 저장 (각 브랜드는 독립적으로 작동)
                                    # 월 이름을 숫자로 변환 (9월 → 9)
                                    month_num = month_idx + 9 if month_idx < 4 else month_idx - 3
                                    save_data.append({
                                        'season': '25FW',
                                        'month': month_num,
                                        'brand': brand,
                                        'target_quantity': int(value)
                                    })
                            
                            # 원본 형식으로 저장
                            save_df = pd.DataFrame(save_data)
                            save_df.to_csv(MONTHLY_TARGETS_FILE, index=False, encoding="utf-8")
                            
                            st.success("✅ 배정수량이 성공적으로 저장되었습니다!")
                            
                            # 저장 완료 후 상태 업데이트 (새로고침 없이)
                            st.session_state['data_updated'] = True
                            
                        except Exception as e:
                            st.error(f"❌ 저장 중 오류 발생: {str(e)}")
                            st.info("💡 파일 권한을 확인해주세요.")
                
                with col2:
                    if st.button("🚀 자동배정실행", type="secondary", use_container_width=True):
                        # 자동배정 실행
                        execute_monthly_automatic_assignment_from_table(edited_df)
                
                with col3:
                    pass  # 빈 공간
                
                with col4:
                    pass  # 빈 공간
                
                # 배정요청수량 vs 배정수량 비교 테이블
                st.markdown("---")
                st.subheader("📊 배정요청수량 vs 배정수량 비교")
                
                try:
                    # 배정 이력에서 실제 배정된 수량 계산
                    if os.path.exists(ASSIGNMENT_FILE):
                        assignment_df = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
                        
                        # 25FW 시즌의 브랜드별 배정수량 계산 (9~2월)
                        brand_assigned = {}
                        for brand in available_brands:
                            # 25FW 시즌의 모든 월(9, 10, 11, 12, 1, 2월) 배정 이력 찾기
                            season_assignments = assignment_df[
                                (assignment_df['브랜드'] == brand) & 
                                (assignment_df['배정월'].isin(FW_MONTHS))
                            ]
                            brand_assigned[brand] = len(season_assignments)
                        
                        # 브랜드별 비교 데이터프레임 생성
                        comparison_data = []
                        for brand in available_brands:
                            # 25FW 시즌의 총 요청수량 계산
                            requested_qty = targets_df[
                                (targets_df['season'] == '25FW') & 
                                (targets_df['brand'] == brand)
                            ]['target_quantity'].sum()
                            
                            assigned_qty = brand_assigned.get(brand, 0)
                            difference = requested_qty - assigned_qty
                            
                            comparison_data.append({
                                COLUMN_NAMES['brand']: brand,
                                COLUMN_NAMES['target_quantity']: requested_qty,
                                COLUMN_NAMES['assigned_quantity']: assigned_qty,
                                COLUMN_NAMES['difference']: difference,
                                COLUMN_NAMES['status']: '✅ 완료' if difference == 0 else f'❌ 부족 {difference}건' if difference > 0 else f'⚠️ 초과 {abs(difference)}건'
                            })
                        
                        comparison_df = pd.DataFrame(comparison_data)
                        st.dataframe(comparison_df, use_container_width=True, hide_index=True)
                        
                        # 월별 브랜드별 배정현황 요약
                        st.markdown("---")
                        st.subheader("📊 월별 브랜드별 배정현황 요약")
                        
                        # 월별 브랜드별 상세 현황 표시
                        if 'assignment_history.csv' in os.listdir('data'):
                            try:
                                # targets_df 로드 (변수 스코프 문제 해결)
                                targets_df = pd.read_csv(MONTHLY_TARGETS_FILE, encoding='utf-8')
                                
                                history_df = pd.read_csv('data/assignment_history.csv', encoding='utf-8')
                                if not history_df.empty and '브랜드' in history_df.columns and '배정월' in history_df.columns:
                                    # 월별 브랜드별 배정 현황 집계
                                    monthly_brand_summary = history_df.groupby(['브랜드', '배정월']).size().reset_index(name='실제')
                                    
                                    # 월별 브랜드별 목표 수량과 비교
                                    summary_data = []
                                    for _, row in monthly_brand_summary.iterrows():
                                        brand = row['브랜드']
                                        month = row['배정월']
                                        actual = row['실제']
                                        
                                        # 월 형식 변환: "9월" → "9", "10월" → "10"
                                        month_number = int(month.replace('월', ''))
                                        
                                        # 해당 월의 목표 수량 찾기
                                        target_row = targets_df[
                                            (targets_df['season'] == '25FW') & 
                                            (targets_df['brand'] == brand) & 
                                            (targets_df['month'] == month_number)
                                        ]
                                        
                                        if len(target_row) > 0:
                                            target = target_row['target_quantity'].iloc[0]
                                        else:
                                            target = 0
                                        
                                        # 상태 아이콘 결정
                                        if actual == target:
                                            status = "✅"
                                        elif actual < target:
                                            status = "⚠️"
                                        else:
                                            status = "❌"
                                        
                                        summary_data.append({
                                            '브랜드': brand,
                                            '월': month,
                                            '배정요청수량': target,
                                            '배정수량': actual,
                                            '상태': status
                                        })
                                    
                                    if summary_data:
                                        summary_df = pd.DataFrame(summary_data)
                                        # 브랜드 순서 정렬 (MLB, DX, DV, ST 순서로)
                                        brand_order = {'MLB': 1, 'DX': 2, 'DV': 3, 'ST': 4}
                                        summary_df['브랜드_순서'] = summary_df['브랜드'].map(brand_order)
                                        summary_df = summary_df.sort_values(['브랜드_순서', '월'])
                                        summary_df = summary_df.drop('브랜드_순서', axis=1)
                                        
                                        st.dataframe(summary_df, use_container_width=True, hide_index=True)
                                        
                                        # 요약 통계
                                        total_requested = summary_df['배정요청수량'].sum()
                                        total_assigned = summary_df['배정수량'].sum()
                                        st.info(f"📈 **전체 요약**: 배정요청수량 {total_requested}건, 배정수량 {total_assigned}건, 차이 {total_assigned - total_requested:+d}건")
                                    else:
                                        st.info("📋 월별 브랜드별 배정 현황 데이터가 없습니다.")
                                else:
                                    st.info("📋 배정 이력 파일의 형식이 올바르지 않습니다.")
                            except Exception as e:
                                st.warning(f"⚠️ 월별 브랜드별 현황 분석 중 오류: {str(e)}")
                        else:
                            st.info("📋 배정 이력 파일이 없어 월별 브랜드별 현황을 분석할 수 없습니다.")
                        
                        # 배정 피드백 (25FW 시즌)
                        st.markdown("---")
                        st.subheader("🔄 25FW 시즌 배정 피드백")
                        
                        for _, row in comparison_df.iterrows():
                            brand = row[COLUMN_NAMES['brand']]
                            requested = row[COLUMN_NAMES['target_quantity']]
                            assigned = row[COLUMN_NAMES['assigned_quantity']]
                            difference = row[COLUMN_NAMES['difference']]
                            
                            if difference == 0:
                                st.success(f"**{brand}**: 정확한 배정 완료 ✅")
                            elif difference > 0:
                                st.warning(f"**{brand}**: {difference}건 부족 - 추가 배정 필요 ⚠️")
                            else:
                                st.info(f"**{brand}**: {abs(difference)}건 초과 배정 - 계약수량 초과 ⚠️")
                        
                    else:
                        st.info("📋 배정 이력이 없어 비교할 수 없습니다.")
                        
                except Exception as e:
                    st.error(f"❌ 비교 분석 중 오류: {str(e)}")
                
            else:
                st.warning("⚠️ 파일 형식이 올바르지 않습니다. 기존 데이터를 백업하고 기본 데이터를 생성합니다.")
                
                # 기존 데이터 백업
                backup_file = MONTHLY_TARGETS_FILE.replace('.csv', '_backup.csv')
                try:
                    targets_df.to_csv(backup_file, index=False, encoding="utf-8")
                    st.info(f"💾 기존 데이터가 {backup_file}에 백업되었습니다.")
                except:
                    st.warning("⚠️ 기존 데이터 백업에 실패했습니다.")
                
                # 기본 데이터 생성
                default_data = create_default_monthly_targets()
                if default_data is not None:
                    st.success("✅ 기본 데이터가 생성되었습니다. 페이지를 새로고침해주세요.")
                    return
                else:
                    st.error("❌ 기본 데이터 생성에 실패했습니다.")
                    return
                
        except Exception as e:
            st.error(f"❌ 데이터 로드 중 오류: {str(e)}")
            st.info("💡 기본 데이터를 생성합니다.")
            
            # 오류 발생 시 기본 데이터 생성
            default_data = create_default_monthly_targets()
            if default_data is not None:
                st.success("✅ 기본 데이터가 생성되었습니다. 페이지를 새로고침해주세요.")
                return
            else:
                st.error("❌ 기본 데이터 생성에 실패했습니다.")
                return
    else:
        st.warning("⚠️ 월별 배정수량 파일이 없습니다. 기존 데이터를 찾아보고 기본 데이터를 생성합니다.")
        
        # 기존 데이터 파일 찾기 (다른 이름으로 저장된 파일들)
        possible_files = [
            "data/monthly_targets.csv",
            "data/assignment_targets.csv", 
            "data/targets.csv",
            "data/monthly_assignment.csv"
        ]
        
        existing_data = None
        for file_path in possible_files:
            if os.path.exists(file_path):
                try:
                    temp_df = pd.read_csv(file_path)
                    if 'month' in temp_df.columns and 'brand' in temp_df.columns and 'target_quantity' in temp_df.columns:
                        existing_data = temp_df
                        st.info(f"💾 기존 데이터를 {file_path}에서 찾았습니다. 복원합니다.")
                        break
                except:
                    continue
        
        if existing_data is not None:
            # 기존 데이터 복원
            existing_data.to_csv(MONTHLY_TARGETS_FILE, index=False, encoding="utf-8")
            st.success("✅ 기존 데이터가 복원되었습니다. 페이지를 새로고침해주세요.")
            return
        else:
            # 기본 데이터 생성
            st.info("💡 기존 데이터를 찾을 수 없어 기본 데이터를 생성합니다.")
            default_data = create_default_monthly_targets()
            if default_data is not None:
                st.success("✅ 기본 데이터가 생성되었습니다. 페이지를 새로고침해주세요.")
                return
            else:
                st.error("❌ 기본 데이터 생성에 실패했습니다.")
                return

def create_default_monthly_targets():
    """기본 월별 배정수량 데이터 생성 (모든 브랜드 포함, 0값으로 초기화)"""
    try:
        # 25FW 시즌 기본 데이터 생성 (모든 브랜드와 월을 포함하여 0값으로 설정)
        default_data = []
        months = [9, 10, 11, 12, 1, 2]
        brands = ['MLB', 'DX', 'DV', 'ST']  # 모든 브랜드 포함
        
        for month in months:
            for brand in brands:
                default_data.append({
                    'season': '25FW',
                    'month': month,
                    'brand': brand,
                    'target_quantity': 0  # 모든 값을 0으로 초기화
                })
        
        # CSV 파일로 저장
        default_df = pd.DataFrame(default_data)
        default_df.to_csv(MONTHLY_TARGETS_FILE, index=False, encoding="utf-8")
        
        return default_df
        
    except Exception as e:
        st.error(f"❌ 기본 데이터 생성 중 오류: {str(e)}")
        return None

def save_monthly_targets(edited_df):
    """편집된 월별 배정수량을 저장"""
    try:
        # 피벗 테이블을 원래 형식으로 변환
        targets_data = []
        for month_idx, month_name in enumerate(edited_df.index):
            month_num = [9, 10, 11, 12, 1, 2][month_idx]
            # 시즌 컬럼은 건너뛰고 브랜드 컬럼만 처리
            for brand in edited_df.columns:
                if brand != '시즌':  # 시즌 컬럼 제외
                    quantity = int(edited_df.loc[month_name, brand])
                    targets_data.append({
                        'year': 2025 if month_num in [9, 10, 11, 12] else 2026,
                        'month': month_num,
                        'brand': brand,
                        'target_quantity': quantity
                    })
        
        # 데이터프레임 생성 및 저장
        new_targets_df = pd.DataFrame(targets_data)
        save_with_auto_sync(new_targets_df, MONTHLY_TARGETS_FILE, "Update monthly assignment targets")
        st.success("✅ 월별 배정수량이 성공적으로 저장되었습니다!")
        
    except Exception as e:
        st.error(f"❌ 저장 중 오류 발생: {e}")

def execute_monthly_automatic_assignment_from_table(edited_df):
    """동시배정 방식으로 모든 월을 한 번에 배정"""
    try:
        # 시작 알람 (3초 후 자동 제거)
        start_container = st.info("🚀 동시배정을 시작합니다...")
        time.sleep(3)
        start_container.empty()
        
        # 배정 이력 초기화 (기존 배정 데이터 삭제)
        if os.path.exists(ASSIGNMENT_FILE):
            os.remove(ASSIGNMENT_FILE)
        
        # Excel → CSV 실시간 동기화 후 influencer.csv에서 데이터 로드
        excel_file_path = "data/fnfcrew"  # data 디렉토리의 fnfcrew 파일
        csv_file_path = "data/influencer.csv"
        
        # Excel 파일이 있으면 강제 동기화, 없으면 CSV 직접 사용
        if os.path.exists(excel_file_path):
            try:
                # Excel 파일 강제 읽기 및 동기화
                excel_df = pd.read_excel(excel_file_path, sheet_name="인플루언서", engine="openpyxl")
                
                # 필수 컬럼 확인
                required_columns = ['id', 'name', 'follower', 'unit_fee', 'mlb_qty', 'dx_qty', 'dv_qty', 'st_qty']
                missing_columns = [col for col in required_columns if col not in excel_df.columns]
                
                if missing_columns:
                    st.error("❌ Excel 파일에 필요한 데이터가 누락되었습니다.")
                    return
                
                # 데이터 전처리
                qty_columns = ['mlb_qty', 'dx_qty', 'dv_qty', 'st_qty']
                for col in qty_columns:
                    if col in excel_df.columns:
                        excel_df[col] = excel_df[col].fillna(0).astype(int)
                
                if 'follower' in excel_df.columns:
                    excel_df['follower'] = excel_df['follower'].fillna(0).astype(int)
                if 'unit_fee' in excel_df.columns:
                    excel_df['unit_fee'] = excel_df['unit_fee'].fillna(0).astype(int)
                
                # CSV로 강제 동기화 (최신 데이터 보장)
                excel_df.to_csv(csv_file_path, index=False, encoding="utf-8")
                influencer_df = excel_df
                
                # 동기화 완료 메시지
                st.success("✅ Excel 파일이 influencer.csv에 최신화되었습니다!")
                
                # MLB 계약수 총합 표시 (디버깅용)
                if 'mlb_qty' in excel_df.columns:
                    mlb_total = excel_df['mlb_qty'].sum()
                    st.info(f"📊 MLB 총 계약수: {mlb_total:,}건")
                
            except Exception as e:
                st.error(f"❌ Excel 파일 처리 중 오류: {str(e)}")
                return
        else:
            # Excel 파일이 없으면 CSV 사용
            if not os.path.exists(csv_file_path):
                st.error("❌ 데이터 파일을 찾을 수 없습니다.")
                return
            
            influencer_df = pd.read_csv(csv_file_path, encoding="utf-8")
        
        # 배정 데이터 생성
        assignment_data = []
        
        # 브랜드별 월별 배정 카운터 초기화
        brand_month_assigned_count = {}
        for brand in edited_df.columns:
            brand_month_assigned_count[brand] = {}
            for month in edited_df.index:
                brand_month_assigned_count[brand][month] = 0
        
        # 인플루언서별 브랜드 잔여수 계산
        influencer_brand_remaining_qty = {}
        for _, influencer in influencer_df.iterrows():
            influencer_id = influencer['id']
            influencer_brand_remaining_qty[influencer_id] = {}
            for brand in edited_df.columns:
                brand_qty_col = f"{brand.lower()}_qty"
                if brand_qty_col in influencer_df.columns:
                    influencer_brand_remaining_qty[influencer_id][brand] = influencer[brand_qty_col]
                else:
                    influencer_brand_remaining_qty[influencer_id][brand] = 0
        
        # 동시배정을 위한 인플루언서 우선순위 결정
        # 잔여수가 많은 인플루언서부터 우선 배정
        influencer_priority = []
        for influencer_id, brand_data in influencer_brand_remaining_qty.items():
            total_remaining = sum(brand_data.values())
            if total_remaining > 0:
                influencer_priority.append((influencer_id, total_remaining))
        
        # 잔여수가 많은 순서로 정렬
        influencer_priority.sort(key=lambda x: x[1], reverse=True)
        
        # 디버깅 정보 출력
        st.info(f"📊 배정 정보:")
        st.write(f"  총 인플루언서: {len(influencer_priority)}명")
        st.write(f"  MLB 총 계약수: {sum([data['MLB'] for data in influencer_brand_remaining_qty.values()])}개")
        
        # 간단하고 명확한 배정 로직
        # 1단계: 각 브랜드의 총 계약수와 월별 목표 파악
        brand_total_contracts = {}
        brand_month_targets = {}
        
        for brand in edited_df.columns:
            brand_qty_col = f"{brand.lower()}_qty"
            if brand_qty_col in influencer_df.columns:
                brand_total_contracts[brand] = influencer_df[brand_qty_col].sum()
                brand_month_targets[brand] = {}
                for month in edited_df.index:
                    brand_month_targets[brand][month] = int(edited_df.loc[month, brand])
            else:
                brand_total_contracts[brand] = 0
                brand_month_targets[brand] = {}
                for month in edited_df.index:
                    brand_month_targets[brand][month] = 0
        
        # 배정 정보 출력
        st.info(f"📊 배정 정보:")
        st.write(f"  MLB 총 계약수: {brand_total_contracts.get('MLB', 0)}개")
        st.write(f"  MLB 월별 목표: {brand_month_targets.get('MLB', {})}")
        
        # 2단계: 정확한 검증과 최적 배정 구현
        # 인플루언서별 브랜드 배정 횟수를 정확히 추적
        influencer_brand_assigned_count = {}
        
        for brand in edited_df.columns:
            if brand_total_contracts[brand] <= 0:
                continue
                
            # 해당 브랜드의 계약수가 있는 인플루언서들 (잔여수 많은 순)
            available_influencers = []
            for influencer_id, brand_data in influencer_brand_remaining_qty.items():
                if brand_data[brand] > 0:
                    available_influencers.append((influencer_id, brand_data[brand]))
            
            # 잔여수가 많은 순서로 정렬
            available_influencers.sort(key=lambda x: x[1], reverse=True)
            
            # 3단계: 각 인플루언서의 계약수를 정확히 추적하며 배정
            for influencer_id, remaining_qty in available_influencers:
                influencer = influencer_df[influencer_df['id'] == influencer_id].iloc[0]
                brand_qty_col = f"{brand.lower()}_qty"
                original_contract_qty = influencer[brand_qty_col]
                
                # 해당 인플루언서가 이미 이 브랜드로 몇 번 배정되었는지 확인
                if influencer_id not in influencer_brand_assigned_count:
                    influencer_brand_assigned_count[influencer_id] = {}
                if brand not in influencer_brand_assigned_count[influencer_id]:
                    influencer_brand_assigned_count[influencer_id][brand] = 0
                
                current_assigned_count = influencer_brand_assigned_count[influencer_id][brand]
                
                # 🚨 핵심 제약: 계약수를 초과하지 않도록 정확히 체크
                if current_assigned_count >= original_contract_qty:
                    continue  # 이미 계약수만큼 배정됨
                
                # 해당 인플루언서의 계약수를 모든 월에 걸쳐서 배정
                for month_name in edited_df.index:
                    # 계약수를 모두 사용했으면 중단
                    if current_assigned_count >= original_contract_qty:
                        break
                        
                    target_quantity = brand_month_targets[brand][month_name]
                    if target_quantity <= 0:
                        continue
                    
                    # 월별 목표 초과 방지
                    if brand_month_assigned_count[brand][month_name] >= target_quantity:
                        continue
                    
                    # 해당 인플루언서가 이미 이 월에 배정되었는지 확인
                    already_assigned = any(
                        assignment['id'] == influencer_id and 
                        assignment['브랜드'] == brand and 
                        assignment['배정월'] == month_name 
                        for assignment in assignment_data
                    )
                    
                    if already_assigned:
                        continue
                    
                    # 배정 실행
                    assignment_info = {
                        '브랜드': brand,
                        'id': influencer['id'],
                        '이름': influencer['name'],
                        '배정월': month_name,
                        'FLW': influencer['follower'],
                        '1회계약단가': influencer.get('unit_fee', 0),
                        '2차활용': influencer.get('sec_usage', ''),
                        '브랜드_계약수': influencer[brand_qty_col],
                        '브랜드_실집행수': 0,
                        '브랜드_잔여수': original_contract_qty - (current_assigned_count + 1),
                        '전체_계약수': influencer['total_qty'],
                        '전체_실집행수': 0,
                        '전체_잔여수': influencer['total_qty'] - 1,
                        '집행URL': '',
                        '상태': '📋 배정완료'
                    }
                    
                    assignment_data.append(assignment_info)
                    
                    # 카운터 업데이트
                    brand_month_assigned_count[brand][month_name] += 1
                    influencer_brand_assigned_count[influencer_id][brand] += 1
                    current_assigned_count += 1
                    
                    # 해당 월의 목표 수량에 도달하면 다음 월로
                    if brand_month_assigned_count[brand][month_name] >= target_quantity:
                        continue
        
        # 배정 결과 저장
        if assignment_data:
            # DataFrame으로 변환
            assignment_df = pd.DataFrame(assignment_data)
            
            # CSV로 저장
            assignment_df.to_csv(ASSIGNMENT_FILE, index=False, encoding="utf-8")
            
            # 성공 메시지
            st.success(f"✅ 동시배정이 완료되었습니다! 총 {len(assignment_data)}건의 배정이 생성되었습니다.")
            
            # 배정 현황 요약 표시
            st.subheader("📊 배정 현황 요약")
            
            # 브랜드별 월별 배정 현황
            summary_data = []
            for brand in edited_df.columns:
                for month in edited_df.index:
                    target = int(edited_df.loc[month, brand])
                    actual = brand_month_assigned_count[brand][month]
                    status = "✅" if actual >= target else "❌"
                    summary_data.append({
                        '브랜드': brand,
                        '월': month,
                        '목표': target,
                        '실제': actual,
                        '상태': status
                    })
            
            summary_df = pd.DataFrame(summary_data)
            st.dataframe(summary_df, use_container_width=True)
            
            # 초과 배정 경고
            over_assigned = []
            for brand in edited_df.columns:
                total_target = edited_df[brand].sum()
                total_actual = sum(brand_month_assigned_count[brand].values())
                if total_actual > total_target:
                    over_assigned.append({
                        '브랜드': brand,
                        '요청수량': total_target,
                        '배정수량': total_actual,
                        '초과': total_actual - total_target
                    })
            
            if over_assigned:
                st.warning("⚠️ 초과 배정이 발생했습니다!")
                over_df = pd.DataFrame(over_assigned)
                st.dataframe(over_df, use_container_width=True)
            else:
                st.success("✅ 모든 브랜드가 요청된 수량 이내로 배정되었습니다!")
            
        else:
            st.warning("⚠️ 배정 가능한 인플루언서가 없습니다.")
        
    except Exception as e:
        st.error(f"❌ 자동배정 실행 중 오류 발생: {str(e)}")
        st.info("�� 데이터 형식을 확인해주세요.")

def execute_monthly_automatic_assignment(edited_df, df):
    """기존 자동배정 함수 (호환성을 위해 유지)"""
    pass

def update_assignment_feedback_after_execution(execution_month):
    """실집행 완료 후 배정피드백 자동 업데이트"""
    try:
        # 배정 이력과 집행 상태 로드
        if not os.path.exists(ASSIGNMENT_FILE) or not os.path.exists(EXECUTION_FILE):
            return
        
        assignment_df = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        execution_df = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
        
        # 해당 월의 실집행 완료 데이터 필터링
        execution_completed = execution_df[
            (execution_df['배정월'] == execution_month) & 
            (execution_df['상태'] == '✅ 집행완료')
        ].copy()
        
        if execution_completed.empty:
            return
        
        # 인플루언서별 잔여수 재계산
        influencer_remaining_qty = {}
        
        # 원본 계약수 로드
        if os.path.exists(INFLUENCER_FILE):
            influencer_df = pd.read_csv(INFLUENCER_FILE, encoding="utf-8")
            
            for _, influencer in influencer_df.iterrows():
                influencer_id = influencer['id']
                influencer_remaining_qty[influencer_id] = {}
                
                for brand in ['MLB', 'DX', 'DV', 'ST']:
                    brand_qty_col = f"{brand.lower()}_qty"
                    if brand_qty_col in influencer_df.columns:
                        influencer_remaining_qty[influencer_id][brand] = influencer[brand_qty_col]
                    else:
                        influencer_remaining_qty[influencer_id][brand] = 0
        
        # 실집행 완료로 인한 잔여수 감소
        for _, execution in execution_completed.iterrows():
            influencer_id = execution['id']
            brand = execution['브랜드']
            
            if influencer_id in influencer_remaining_qty and brand in influencer_remaining_qty[influencer_id]:
                influencer_remaining_qty[influencer_id][brand] -= 1
        
        # 10~2월 배정내역을 바탕으로 잔여수 부족 확인
        future_months = ['10월', '11월', '12월', '1월', '2월']
        if execution_month in future_months:
            future_months.remove(execution_month)
        
        future_assignments = assignment_df[assignment_df['배정월'].isin(future_months)]
        
        # 잔여수 부족 인플루언서 식별
        insufficient_influencers = []
        
        for _, assignment in future_assignments.iterrows():
            influencer_id = assignment['id']
            brand = assignment['브랜드']
            
            if (influencer_id in influencer_remaining_qty and 
                brand in influencer_remaining_qty[influencer_id] and
                influencer_remaining_qty[influencer_id][brand] < 0):
                
                insufficient_influencers.append({
                    'id': influencer_id,
                    '이름': assignment['이름'],
                    '브랜드': brand,
                    '배정월': assignment['배정월'],
                    '원래_계약수': assignment['브랜드_계약수'],
                    '현재_잔여수': influencer_remaining_qty[influencer_id][brand],
                    '부족_수량': abs(influencer_remaining_qty[influencer_id][brand])
                })
        
        # 배정피드백 파일에 업데이트
        if insufficient_influencers:
            feedback_file = "data/assignment_feedback.csv"
            feedback_data = []
            
            for item in insufficient_influencers:
                feedback_data.append({
                    '업데이트_일시': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    '실행_월': execution_month,
                    '인플루언서_ID': item['id'],
                    '인플루언서_이름': item['이름'],
                    '브랜드': item['브랜드'],
                    '배정_월': item['배정월'],
                    '원래_계약수': item['원래_계약수'],
                    '현재_잔여수': item['현재_잔여수'],
                    '부족_수량': item['부족_수량'],
                    '상태': '⚠️ 잔여수 부족',
                    '조치_필요': '재배정 또는 계약수 추가 필요'
                })
            
            feedback_df = pd.DataFrame(feedback_data)
            
            # 기존 피드백이 있으면 추가, 없으면 새로 생성
            if os.path.exists(feedback_file):
                existing_feedback = pd.read_csv(feedback_file, encoding="utf-8")
                updated_feedback = pd.concat([existing_feedback, feedback_df], ignore_index=True)
            else:
                updated_feedback = feedback_df
            
            updated_feedback.to_csv(feedback_file, index=False, encoding="utf-8")
            
            # 성공 메시지 (3초 후 자동 제거)
            success_container = st.success(f"✅ {execution_month} 실집행 완료 후 배정피드백이 업데이트되었습니다!")
            time.sleep(3)
            success_container.empty()
            
    except Exception as e:
        st.error(f"❌ 배정피드백 업데이트 중 오류 발생: {str(e)}")
        st.info("�� 잠시 후 다시 시도해주세요.")

def main():
    # 페이지 설정
    st.set_page_config(page_title="인플루언서 배정 앱", layout="wide")
    load_css()
    
    st.title("🎯 인플루언서 배정 앱")
    
    # 앱 시작 시 GitHub에서 최신 데이터 가져오기 (클라우드에서만)
    if 'data_synced' not in st.session_state:
        # 클라우드에서만 자동 동기화 실행
        if is_running_on_streamlit_cloud():
            with st.spinner("🔄 GitHub에서 최신 데이터를 가져오는 중..."):
                # 조용히 데이터 가져오기 (알림 없이)
                try:
                    result = subprocess.run(['git', 'pull', 'origin', 'master'], 
                                          capture_output=True, text=True, cwd=SCRIPT_DIR)
                except Exception as e:
                    pass  # 오류가 있어도 조용히 처리
        else:
            # 로컬에서는 자동 동기화 비활성화
            st.info("💻 로컬 환경에서 실행 중입니다. (자동 GitHub 동기화 비활성화)")
        st.session_state.data_synced = True
    
    # 데이터 로드
    df = load_influencer_data()
    if df is None:
        return
    
    # 사이드바 렌더링
    selected_month, selected_season, month_options = render_sidebar(df)
    
    # 탭 상태 초기화
    if 'current_tab' not in st.session_state:
        st.session_state.current_tab = 0
    
    # 탭 생성
    tab1, tab2, tab3 = st.tabs(["👥 인플루언서별", "📊 배정 및 집행상태", "🎯 배정수량관리"])
    
    # 현재 탭 상태 업데이트
    if tab1:
        st.session_state.current_tab = 0
    elif tab2:
        st.session_state.current_tab = 1
    elif tab3:
        st.session_state.current_tab = 2
    
    with tab1:
        render_influencer_tab(df)
    
    with tab2:
        render_assignment_results_tab(month_options, df)
    
    with tab3:
        render_monthly_targets_tab(df)

if __name__ == "__main__":
    main()
