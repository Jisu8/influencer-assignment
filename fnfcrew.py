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
    return os.environ.get('STREAMLIT_SERVER_HEADLESS', 'false').lower() == 'true'

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

# 데이터 디렉토리가 없으면 생성
os.makedirs(DATA_DIR, exist_ok=True)

# =============================================================================
# GitHub Actions 자동 동기화 기능
# =============================================================================

def trigger_github_workflow(commit_message="Auto-update data files"):
    """GitHub Actions 워크플로우 트리거"""
    try:
        # GitHub Personal Access Token (Streamlit Secrets에서 가져오기)
        github_token = st.secrets.get("GITHUB_TOKEN", "")
        repo_owner = st.secrets.get("GITHUB_REPO_OWNER", "jisu8")
        repo_name = st.secrets.get("GITHUB_REPO_NAME", "influencer-assignment")
        
        if not github_token:
            st.warning("⚠️ GitHub 토큰이 설정되지 않았습니다. 로컬에만 저장됩니다.")
            return False
        
        # GitHub API로 워크플로우 트리거
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/dispatches"
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        data = {
            "event_type": "data_update",
            "client_payload": {
                "message": commit_message,
                "timestamp": datetime.now().isoformat()
            }
        }
        
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 204:
            st.success("✅ GitHub에 데이터가 자동 동기화되었습니다!")
            return True
        else:
            st.error(f"❌ GitHub 동기화 실패: {response.status_code}")
            return False
            
    except Exception as e:
        st.error(f"❌ GitHub 동기화 중 오류: {e}")
        return False

def save_with_auto_sync(data, file_path, commit_message=None):
    """데이터 저장 후 GitHub Actions로 자동 동기화 (클라우드에서만)"""
    try:
        # 로컬에 데이터 저장
        data.to_csv(file_path, index=False, encoding="utf-8")
        
        # 클라우드에서만 GitHub 동기화 실행
        if is_running_on_streamlit_cloud():
            # 커밋 메시지 생성
            if commit_message is None:
                filename = os.path.basename(file_path)
                commit_message = f"Auto-update {filename}"
            
            # GitHub Actions 트리거
            sync_success = trigger_github_workflow(commit_message)
            
            if not sync_success:
                st.warning("⚠️ GitHub 동기화에 실패했습니다. 수동으로 데이터를 백업해주세요.")
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
        # Git 상태 확인
        result = subprocess.run(['git', 'status', '--porcelain'], 
                              capture_output=True, text=True, cwd=SCRIPT_DIR)
        
        if result.stdout.strip():  # 변경사항이 있는 경우
            # 변경사항 추가
            subprocess.run(['git', 'add', '.'], cwd=SCRIPT_DIR, check=True)
            
            # 커밋
            subprocess.run(['git', 'commit', '-m', commit_message], 
                         cwd=SCRIPT_DIR, check=True)
            
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

# 상태 옵션
STATUS_OPTIONS = ["📋 배정완료", "✅ 집행완료"]

# =============================================================================
# CSS 스타일
# =============================================================================

def load_css():
    """CSS 스타일 로드"""
    st.markdown("""
    <style>
        /* 전체 텍스트 크기 줄이기 */
        .stMarkdown, .stText, .stSelectbox, .stNumberInput, .stButton, .stDataFrame {
            font-size: 0.9em !important;
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

def add_execution_data(df, execution_file=EXECUTION_FILE):
    """실행 데이터를 DataFrame에 추가하고 잔여수 계산"""
    if os.path.exists(execution_file):
        execution_data = pd.read_csv(execution_file, encoding="utf-8")
        if not execution_data.empty:
            # 브랜드별 실행수 계산
            brand_execution = execution_data.groupby(["ID", "브랜드"])["실제집행수"].sum().reset_index()
            brand_execution.columns = ["ID", "브랜드", "브랜드_실집행수"]
            
            # 기존 컬럼이 있으면 제거
            if "브랜드_실집행수" in df.columns:
                df = df.drop("브랜드_실집행수", axis=1)
            
            # 병합
            df = df.merge(brand_execution, on=["ID", "브랜드"], how="left")
            df["브랜드_실집행수"] = df["브랜드_실집행수"].fillna(0)
            
            # 잔여수 재계산
            if "브랜드_계약수" in df.columns and "브랜드_실집행수" in df.columns:
                df["브랜드_잔여수"] = df["브랜드_계약수"] - df["브랜드_실집행수"]
                df["브랜드_잔여수"] = df["브랜드_잔여수"].clip(lower=0)
        else:
            df["브랜드_실집행수"] = 0
    else:
        df["브랜드_실집행수"] = 0
    
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
        st.rerun()
    
    # 브랜드별로 결과 정리
    brand_assignments = {"MLB": [], "DX": [], "DV": [], "ST": []}
    for assignment in incomplete_assignments:
        if "(" in assignment and ")" in assignment:
            brand = assignment.split("(")[1].split(")")[0]
            if brand in brand_assignments:
                brand_assignments[brand].append(assignment.split(" (")[0])
    
    # 브랜드별로 결과 표시
    for brand in BRANDS:
        if brand_assignments[brand]:
            st.markdown(f'<div class="brand-list"><div class="brand-title">{brand}</div>', unsafe_allow_html=True)
            for name in brand_assignments[brand]:
                influencer_id = df[df['name'] == name]['id'].iloc[0] if not df[df['name'] == name].empty else "ID 없음"
                st.markdown(f'<div class="influencer-item">• {name}, {influencer_id}</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
    
    st.stop()

# =============================================================================
# 배정 관련 함수들
# =============================================================================

def execute_automatic_assignment(selected_month, selected_season, quantities, df):
    """자동 배정 실행"""
    # 이전 달 완료 상태 확인
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
            influencer_id = row['ID']
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
            brand_df = brand_df.sort_values("follower", ascending=False)
            
            assigned_count = 0
            for _, row in brand_df.iterrows():
                if assigned_count >= qty:
                    break
                
                # 배정 핵심 로직: 전월까지 집행완료 + 현재까지 배정을 고려한 잔여수 확인
                brand_contract_qty = row[f"{brand.lower()}_qty"]
                
                # 1. 전월까지의 모든 집행완료 데이터 확인
                execution_data = load_execution_data()
                total_executed_count = 0
                if not execution_data.empty:
                    # 해당 인플루언서의 해당 브랜드 모든 집행완료 수
                    exec_mask = (
                        (execution_data['id'] == row['id']) &
                        (execution_data['브랜드'] == brand)
                    )
                    if exec_mask.any():
                        total_executed_count = execution_data.loc[exec_mask, '실제집행수'].sum()
                
                # 2. 현재까지의 모든 배정 수 확인
                if not existing_history.empty and 'id' in existing_history.columns and '브랜드' in existing_history.columns:
                    existing_assignments = existing_history[
                        (existing_history['id'] == row['id']) & 
                        (existing_history['브랜드'] == brand)
                    ]
                else:
                    existing_assignments = pd.DataFrame()
                total_assigned_count = len(existing_assignments)
                
                # 3. 실제 잔여수 계산: 계약수 - (집행완료 + 배정)
                actual_remaining = max(0, brand_contract_qty - total_executed_count - total_assigned_count)
                
                # 4. 잔여수가 없으면 배정 불가
                if actual_remaining <= 0:
                    continue  # 잔여수가 없으면 건너뛰기
                
                # 배정 정보 생성
                assignment_info = create_assignment_info(row, brand, selected_month, df)
                results.append(assignment_info)
                
                newly_assigned_influencers.add(row["id"])
                assigned_count += 1
    
    # 결과 저장
    if results:
        save_assignments(results, existing_history)
        st.rerun()
    else:
        st.warning(f"⚠️ {selected_month}에 배정할 수 있는 인플루언서가 없습니다.")

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
    
    # 잔여수 계산
    brand_remaining = max(0, original_brand_qty - brand_execution_count)
    total_remaining = max(0, original_total_qty - total_execution_count)
    
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
        "집행URL": ""
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
    
    # 로컬에만 저장 (GitHub 동기화 없음)
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
        
        # 중복 배정 확인 (같은 브랜드, 같은 월)
        # 브랜드 필드에 쉼표가 포함된 경우도 체크
        existing_mask = (
            (assignment_history['id'] == influencer_id) &
            (assignment_history['배정월'] == selected_month) &
            (
                (assignment_history['브랜드'] == brand) |
                (assignment_history['브랜드'].str.contains(brand, na=False)) |
                (assignment_history['브랜드'].str.contains(f"{brand},", na=False)) |
                (assignment_history['브랜드'].str.contains(f", {brand}", na=False))
            )
        )
        
        if not existing_mask.any():
            # 배정 핵심 로직: 전월까지 집행완료 + 현재까지 배정을 고려한 잔여수 확인
            influencer_data = df[df['id'] == influencer_id].iloc[0]
            brand_qty_col = f"{brand.lower()}_qty"
            brand_contract_qty = influencer_data.get(brand_qty_col, 0)
            
            # 1. 전월까지의 모든 집행완료 데이터 확인
            execution_data = load_execution_data()
            total_executed_count = 0
            if not execution_data.empty:
                # 해당 인플루언서의 해당 브랜드 모든 집행완료 수
                exec_mask = (
                    (execution_data['id'] == influencer_id) &
                    (execution_data['브랜드'] == brand)
                )
                if exec_mask.any():
                    total_executed_count = execution_data.loc[exec_mask, '실제집행수'].sum()
            
            # 2. 현재까지의 모든 배정 수 확인
            existing_assignments = assignment_history[
                (assignment_history['id'] == influencer_id) & 
                (assignment_history['브랜드'] == brand)
            ]
            total_assigned_count = len(existing_assignments)
            
            # 3. 실제 잔여수 계산: 계약수 - (집행완료 + 배정)
            actual_remaining = max(0, brand_contract_qty - total_executed_count - total_assigned_count)
            
            # 4. 잔여수가 없으면 배정 불가
            if actual_remaining <= 0:
                st.sidebar.error(f"❌ {influencer_name}의 {brand} 브랜드 잔여수가 없습니다. (계약수: {brand_contract_qty}, 집행완료: {total_executed_count}, 배정: {total_assigned_count})")
                return
            
            # 새로운 배정 추가
            new_assignment = create_manual_assignment_info(influencer_id, brand, selected_month, df)
            assignment_history = pd.concat([assignment_history, pd.DataFrame([new_assignment])], ignore_index=True)
            # 로컬에만 저장 (GitHub 동기화 없음)
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
    
    return {
        '브랜드': brand,
        'id': influencer_id,
        '이름': influencer_data['name'],
        '배정월': selected_month,
        'FLW': influencer_data['follower'],
        '1회계약단가': influencer_data['unit_fee'],
        '2차활용': influencer_data['sec_usage'],
        '브랜드_계약수': brand_contract_qty,
        '브랜드_실집행수': 0,
        '브랜드_잔여수': brand_contract_qty,
        '전체_계약수': total_contract_qty,
        '전체_실집행수': 0,
        '전체_잔여수': total_contract_qty,
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
    
    # 데이터 동기화 (사이드바 맨 하단에 배치)
    st.sidebar.markdown("<hr style='margin: 10px 0; border: 0.5px solid #666;'>", unsafe_allow_html=True)
    if st.sidebar.button("🔄 데이터동기화", key="data_sync", use_container_width=True):
        # 연결 상태 확인
        connection_status = check_github_connection()
        
        # 연결이 성공하면 최신 데이터 가져오기
        if connection_status:
            pull_latest_data_from_github(show_in_sidebar=True)
    
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
                    st.rerun()
    
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
                    st.rerun()

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
                        st.rerun()
                
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
    """배정 및 집행결과 탭 렌더링"""
    st.subheader("📊 배정 및 집행결과")
    
    # 필터
    selected_month_filter = st.selectbox("📅 배정월", month_options, index=0, key="tab1_month_filter")
    selected_brand_filter = st.selectbox("🏷️ 브랜드", BRAND_OPTIONS, index=0, key="tab1_brand_filter")
    
    # 배정 결과 로드 및 표시
    if os.path.exists(ASSIGNMENT_FILE):
        assignment_history = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        
        if not assignment_history.empty:
            # 실행 데이터 추가
            all_results = add_execution_data(assignment_history, EXECUTION_FILE)
            

            
            # 필터 적용
            all_results = all_results[all_results["배정월"] == selected_month_filter]
            if selected_brand_filter != "전체":
                all_results = all_results[all_results["브랜드"] == selected_brand_filter]
            
            # 컬럼 순서 정리
            expected_columns = ["브랜드", "ID", "이름", "배정월", "FLW", "브랜드_계약수", 
                              "브랜드_실집행수", "브랜드_잔여수", "전체_계약수", "전체_잔여수"]
            all_results = reorder_columns(all_results, expected_columns)
            
            if not all_results.empty:
                render_assignment_table(all_results, df)
            else:
                st.info("해당 조건의 배정 결과가 없습니다.")
        else:
            st.info("배정 이력이 없습니다.")
    else:
        st.info("배정 이력이 없습니다.")
    
    # 엑셀 업로드 섹션
    render_excel_upload_section(df)

def render_assignment_table(all_results, df):
    """배정 테이블 렌더링"""
    # 체크박스, 넘버, 결과 상태 추가
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
    
    # 결과 상태 추가
    all_results_with_checkbox['결과'] = '📋 배정완료'
    
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
    
    # 컬럼 순서 재정렬 (2차활용 다음에 2차기간, 브랜드_잔여수를 브랜드_계약수 다음에, 결과를 맨 오른쪽에 배치)
    cols = ['선택', '번호', '배정월', '브랜드', 'id', '이름', 'FLW', '1회계약단가', '2차활용', '2차기간', '브랜드_계약수', '브랜드_잔여수', '결과', '집행URL']
    # 존재하는 컬럼만 필터링
    existing_cols = [col for col in cols if col in all_results_with_checkbox.columns]
    # 나머지 컬럼들 추가
    remaining_cols = [col for col in all_results_with_checkbox.columns if col not in existing_cols]
    all_results_with_checkbox = all_results_with_checkbox[existing_cols + remaining_cols]
    
    return all_results_with_checkbox

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
                    all_results_with_checkbox.loc[idx, '결과'] = '✅ 집행완료'

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
    # 요청된 순서: 배정월/브랜드/ID/이름/FLW/2차활용/2차기간/결과/집행URL
    available_columns = ['배정월', '브랜드', 'id', '이름', 'FLW', '2차활용', '2차기간', '결과', '집행URL']
    
    # 누락된 컬럼들을 기본값으로 추가
    download_data = all_results_with_checkbox.copy()
    
    # 2차활용 컬럼이 없으면 기본값 'X'로 추가
    if '2차활용' not in download_data.columns:
        download_data['2차활용'] = 'X'
    
    # 2차기간 컬럼이 없으면 기본값 ''로 추가
    if '2차기간' not in download_data.columns:
        download_data['2차기간'] = ''
    
    # 결과 컬럼이 없으면 기본값 '배정완료'로 추가
    if '결과' not in download_data.columns:
        download_data['결과'] = '배정완료'
    
    # 집행URL 컬럼이 없으면 기본값 ''로 추가
    if '집행URL' not in download_data.columns:
        download_data['집행URL'] = ''
    
    # 요청된 순서대로 컬럼 선택
    existing_columns = [col for col in available_columns if col in download_data.columns]
    download_data = download_data[existing_columns].copy()
    
    if '결과' in download_data.columns:
        download_data['결과'] = download_data['결과'].replace({
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
    # 전체 선택 상태에 따라 체크박스 기본값 설정
    default_checked = st.session_state.get('select_all', False)
    
    return st.data_editor(
        all_results_with_checkbox,
        use_container_width=True,
        hide_index=True,
        key="assignment_data_editor",
        column_config={
            "선택": st.column_config.CheckboxColumn(
                "선택",
                help="실집행완료할 배정을 선택하세요",
                default=default_checked,
                width=10
            ),
            "번호": st.column_config.NumberColumn(
                "번호",
                width=10,
                help="순서 번호",
                format="%d"
            ),
            "결과": st.column_config.SelectboxColumn(
                "결과",
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
            "ID": st.column_config.TextColumn(
                "ID",
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
        
        # 결과 변경사항 처리
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
        st.rerun()

def handle_result_changes(edited_df, all_results_with_checkbox):
    """결과 변경사항 처리"""
    changed_to_executed = []
    changed_to_assigned = []
    
    for idx, row in edited_df.iterrows():
        original_result = all_results_with_checkbox.loc[idx, '결과']
        new_result = row['결과']
        
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
        st.rerun()
    
    if changed_to_assigned:
        update_execution_data(changed_to_assigned, add=False)
        create_success_container(f"✅ {len(changed_to_assigned)}개의 배정이 배정완료로 되돌려졌습니다!", "revert_success")
        st.rerun()

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
    
    # 로컬에만 저장 (GitHub 동기화 없음)
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
    
    # 로컬에만 저장 (GitHub 동기화 없음)
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
    
    # 검증 결과 표시
    if st.session_state.reset_verification_done:
        # 현재 표시된 배정 데이터 가져오기
        current_month_filter = st.session_state.get('tab1_month_filter', '')
        current_brand_filter = st.session_state.get('tab1_brand_filter', '')
        
        # execution_status.csv에서 해당 월의 집행완료 데이터 확인
        has_execution_completed = False
        
        if os.path.exists(EXECUTION_FILE):
            execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
            
            if not execution_data.empty and '배정월' in execution_data.columns and '실제집행수' in execution_data.columns:
                # 실제집행수가 0보다 큰 데이터만 필터링
                completed_data = execution_data[execution_data['실제집행수'] > 0]
                
                if current_month_filter:
                    # 해당 월의 집행완료 데이터만 확인
                    month_completed = completed_data[completed_data['배정월'] == current_month_filter]
                    has_execution_completed = len(month_completed) > 0
                else:
                    # 전체 집행완료 데이터 확인
                    has_execution_completed = len(completed_data) > 0
        
        if has_execution_completed and not st.session_state.reset_confirmation_shown:
            # 경고 메시지와 함께 진행 옵션 제공
            st.warning("⚠️ 집행완료 상태의 배정이 있어 초기화할 수 없습니다. 집행완료를 배정완료로 변경한 후 다시 시도해주세요.")
            st.info("💡 그래도 배정 초기화를 진행하시겠습니까?")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("❌ 취소", key="cancel_reset", use_container_width=True):
                    st.session_state.reset_verification_done = False
                    st.session_state.reset_confirmation_shown = False
                    st.rerun()
            with col2:
                if st.button("✅ 예, 진행합니다", key="proceed_reset", use_container_width=True):
                    st.session_state.reset_confirmation_shown = True
                    # 전체 선택 상태 초기화
                    if 'select_all' in st.session_state:
                        st.session_state.select_all = False
                    
                    # 초기화 실행
                    reset_assignments()
                    
                    # 성공 메시지 표시
                    st.success("✅ 초기화가 완료되었습니다!")
                    
                    # 사용자가 알림을 읽을 수 있도록 3초 대기
                    time.sleep(3)
                    
                    # 상태 초기화
                    st.session_state.reset_verification_done = False
                    st.session_state.reset_confirmation_shown = False
                    st.rerun()
        elif not has_execution_completed:
            # 전체 선택 상태 초기화
            if 'select_all' in st.session_state:
                st.session_state.select_all = False
            
            reset_assignments()
            st.success("✅ 초기화가 완료되었습니다!")
            
            # 사용자가 알림을 읽을 수 있도록 3초 대기
            time.sleep(3)
            
            # 상태 초기화
            st.session_state.reset_verification_done = False
            st.session_state.reset_confirmation_shown = False
            st.rerun()

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
    # 로컬에만 저장 (GitHub 동기화 없음)
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
    st.markdown("💡 **다운로드한 엑셀 파일을 수정한 후 업로드하여 배정 및 실집행결과를 업데이트하세요**")
    
    uploaded_file = st.file_uploader(
        "배정 및 실집행결과 엑셀 파일 업로드",
        type=['xlsx', 'xls'],
        help="수정한 엑셀 파일을 업로드하여 배정 및 실집행결과를 업데이트하세요"
    )
    
    if uploaded_file is not None:
        handle_excel_upload(uploaded_file, df)

def handle_excel_upload(uploaded_file, df):
    """엑셀 업로드 처리"""
    try:
        if uploaded_file.name.endswith('.xlsx'):
            uploaded_data = pd.read_excel(uploaded_file, engine='openpyxl')
        else:
            uploaded_data = pd.read_excel(uploaded_file, engine='xlrd')
        
        # 필수 컬럼만 검증 (id, 배정월, 결과만 필수, 나머지는 선택사항)
        required_columns = ['id', '배정월', '결과']
        missing_columns = [col for col in required_columns if col not in uploaded_data.columns]
        
        if missing_columns:
            st.error(f"❌ 필수 컬럼이 누락되었습니다: {', '.join(missing_columns)}")
        else:
            process_uploaded_data(uploaded_data, df)
            
    except Exception as e:
        st.error(f"❌ 파일 업로드 중 오류가 발생했습니다: {str(e)}")

def process_uploaded_data(uploaded_data, df):
    """업로드된 데이터 처리"""
    # 필수 컬럼 확인
    required_columns = ['id', '배정월', '결과']
    
    # 필수 컬럼이 있으면 처리 진행
    if all(col in uploaded_data.columns for col in required_columns):
        # ID를 기반으로 기본 정보 자동 채우기
        assignment_update_data = uploaded_data[required_columns].copy()
        
        # ID에 따라 기본 정보 자동 채우기
        for idx, row in assignment_update_data.iterrows():
            # id로 인플루언서 정보 찾기
            influencer_info = df[df['id'] == row['id']]
            if not influencer_info.empty:
                # 이름 자동 채우기
                if '이름' not in assignment_update_data.columns:
                    assignment_update_data['이름'] = ''
                assignment_update_data.loc[idx, '이름'] = influencer_info.iloc[0]['name']
                
                # FLW 자동 채우기
                if 'FLW' not in assignment_update_data.columns:
                    assignment_update_data['FLW'] = ''
                assignment_update_data.loc[idx, 'FLW'] = influencer_info.iloc[0]['follower']
                
                # 1회계약단가 자동 채우기
                if '1회계약단가' not in assignment_update_data.columns:
                    assignment_update_data['1회계약단가'] = ''
                assignment_update_data.loc[idx, '1회계약단가'] = influencer_info.iloc[0]['unit_fee']
                
                # 2차활용 자동 채우기
                if '2차활용' not in assignment_update_data.columns:
                    assignment_update_data['2차활용'] = ''
                assignment_update_data.loc[idx, '2차활용'] = influencer_info.iloc[0]['sec_usage']
                
                # 2차기간 자동 채우기
                if '2차기간' not in assignment_update_data.columns:
                    assignment_update_data['2차기간'] = ''
                assignment_update_data.loc[idx, '2차기간'] = influencer_info.iloc[0]['sec_period']
        
        # 브랜드 컬럼이 없으면 기본값 추가
        if '브랜드' not in assignment_update_data.columns:
            assignment_update_data['브랜드'] = 'MLB'  # 기본값
        
        # 집행URL 컬럼이 없으면 빈 값으로 추가
        if '집행URL' not in assignment_update_data.columns:
            assignment_update_data['집행URL'] = ''
        
        update_assignment_history(assignment_update_data, df)
    
    # 실집행수 데이터 업데이트 (브랜드_실집행수 컬럼이 있는 경우에만)
    if '브랜드_실집행수' in uploaded_data.columns:
        execution_update_data = uploaded_data[uploaded_data['브랜드_실집행수'] > 0][['id', '브랜드', '배정월', '브랜드_실집행수']].copy()
        execution_update_data = execution_update_data.rename(columns={'브랜드_실집행수': '실제집행수'})
        execution_update_data = execution_update_data.merge(
            df[['id', 'name']].rename(columns={'id': 'id', 'name': '이름'}),
            on='id',
            how='left'
        )
        update_execution_history(execution_update_data)
    else:
        execution_update_data = pd.DataFrame()
    
    st.success(f"✅ {len(assignment_update_data)}개의 배정 데이터와 {len(execution_update_data)}개의 실집행수 데이터가 업로드되었습니다!")
    
    # 사용자가 알림을 읽을 수 있도록 3초 대기
    time.sleep(3)
    
    # 미리보기
    st.markdown("**업로드된 배정 데이터 미리보기:**")
    st.dataframe(assignment_update_data, use_container_width=True)
    
    if not execution_update_data.empty:
        st.markdown("**업로드된 실집행수 데이터 미리보기:**")
        st.dataframe(execution_update_data, use_container_width=True)
    
    st.rerun()

def update_assignment_history(assignment_update_data, df=None):
    """배정 이력 업데이트"""
    if os.path.exists(ASSIGNMENT_FILE):
        existing_assignment_data = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        if '집행URL' not in existing_assignment_data.columns:
            existing_assignment_data['집행URL'] = ""
    else:
        existing_assignment_data = pd.DataFrame(columns=["브랜드", "ID", "이름", "배정월", "집행URL"])
    
    # ID만 입력된 경우 자동으로 이름, 팔로워, 계약수 등의 정보 채우기 (process_uploaded_data에서 이미 처리된 경우 제외)
    if df is not None:
        for idx, row in assignment_update_data.iterrows():
            # 이름이 비어있거나 FLW가 비어있는 경우에만 자동 채우기
            if (pd.isna(row['이름']) or row['이름'] == "") or ('FLW' in assignment_update_data.columns and (pd.isna(row['FLW']) or row['FLW'] == "")):
                # ID로 인플루언서 정보 찾기
                influencer_info = df[df['id'] == row['id']]
                if not influencer_info.empty:
                    # 이름이 비어있으면 채우기
                    if pd.isna(row['이름']) or row['이름'] == "":
                        assignment_update_data.loc[idx, '이름'] = influencer_info.iloc[0]['name']
                    
                    # FLW 컬럼이 없으면 추가하고 채우기
                    if 'FLW' not in assignment_update_data.columns:
                        assignment_update_data['FLW'] = ""
                    if pd.isna(assignment_update_data.loc[idx, 'FLW']) or assignment_update_data.loc[idx, 'FLW'] == "":
                        assignment_update_data.loc[idx, 'FLW'] = influencer_info.iloc[0]['follower']
                    
                    # 브랜드별 계약수 추가
                    brand_qty_col = f"{row['브랜드'].lower()}_qty"
                    if brand_qty_col in df.columns:
                        if '브랜드_계약수' not in assignment_update_data.columns:
                            assignment_update_data['브랜드_계약수'] = ""
                        assignment_update_data.loc[idx, '브랜드_계약수'] = influencer_info.iloc[0][brand_qty_col]
                    
                    # 1회계약단가 추가
                    if '1회계약단가' not in assignment_update_data.columns:
                        assignment_update_data['1회계약단가'] = ""
                    assignment_update_data.loc[idx, '1회계약단가'] = influencer_info.iloc[0]['unit_fee']
                    
                    # 2차활용 추가
                    if '2차활용' not in assignment_update_data.columns:
                        assignment_update_data['2차활용'] = ""
                    assignment_update_data.loc[idx, '2차활용'] = influencer_info.iloc[0]['sec_usage']
                    
                    # 2차기간 추가
                    if '2차기간' not in assignment_update_data.columns:
                        assignment_update_data['2차기간'] = ""
                    assignment_update_data.loc[idx, '2차기간'] = influencer_info.iloc[0]['sec_period']
    
    combined_assignment_data = pd.concat([existing_assignment_data, assignment_update_data], ignore_index=True)
    combined_assignment_data = combined_assignment_data.drop_duplicates(subset=['id', '브랜드', '배정월'], keep='last')
    # GitHub Actions로 자동 동기화 저장
    save_with_auto_sync(combined_assignment_data, ASSIGNMENT_FILE, "Update assignment history from Excel upload")

def update_execution_history(execution_update_data):
    """실행 이력 업데이트"""
    if os.path.exists(EXECUTION_FILE):
        existing_execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
    else:
        existing_execution_data = pd.DataFrame(columns=["ID", "이름", "브랜드", "배정월", "실제집행수"])
    
    combined_execution_data = pd.concat([existing_execution_data, execution_update_data], ignore_index=True)
    combined_execution_data = combined_execution_data.drop_duplicates(subset=['id', '브랜드', '배정월'], keep='last')
    # GitHub Actions로 자동 동기화 저장
    save_with_auto_sync(combined_execution_data, EXECUTION_FILE, "Update execution history from Excel upload")

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
            render_influencer_table(influencer_summary, selected_brand_filter, influencer_count=len(influencer_summary))
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
        "id": "ID", "name": "이름", "follower": "FLW", "unit_fee": "1회계약단가", "sec_usage": "2차활용", "sec_period": "2차기간"
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
        
        # 브랜드별 집행수와 잔여수 계산
        if os.path.exists(EXECUTION_FILE):
            execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
            if not execution_data.empty and '실제집행수' in execution_data.columns:
                # 해당 브랜드의 집행완료 데이터만 필터링
                brand_executions = execution_data[
                    (execution_data['브랜드'] == selected_brand) & 
                    (execution_data['실제집행수'] > 0)
                ]
                
                # 인플루언서별 해당 브랜드 집행수 계산
                # execution_data의 컬럼명 확인 (id 또는 ID)
                id_column = 'id' if 'id' in brand_executions.columns else 'id'
                brand_executed = brand_executions.groupby(id_column)['실제집행수'].sum()
                influencer_summary[f'{selected_brand}_집행수'] = influencer_summary['id'].map(brand_executed).fillna(0).astype(int)
                
                # 브랜드 잔여수 = 브랜드 계약수 - 브랜드 집행수
                influencer_summary[f'{selected_brand}_잔여수'] = influencer_summary[f'{selected_brand}_계약수'] - influencer_summary[f'{selected_brand}_집행수']
            else:
                influencer_summary[f'{selected_brand}_집행수'] = 0
                influencer_summary[f'{selected_brand}_잔여수'] = influencer_summary[f'{selected_brand}_계약수']
        else:
            influencer_summary[f'{selected_brand}_집행수'] = 0
            influencer_summary[f'{selected_brand}_잔여수'] = influencer_summary[f'{selected_brand}_계약수']
    else:
        # 전체 선택 시 모든 브랜드 계약수 표시
        for brand in BRANDS:
            qty_col = f"{brand.lower()}_qty"
            if qty_col in df.columns:
                influencer_summary[f"{brand}_계약수"] = df.loc[influencer_summary.index, qty_col]
            else:
                influencer_summary[f"{brand}_계약수"] = 0
        
        # 전체 선택 시 전체_집행수와 전체_잔여수 계산
        if os.path.exists(EXECUTION_FILE):
            execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
            if not execution_data.empty and '실제집행수' in execution_data.columns:
                # 모든 브랜드의 집행완료 데이터 필터링
                all_executions = execution_data[execution_data['실제집행수'] > 0]
                
                # 인플루언서별 전체 집행수 계산
                id_column = 'id' if 'id' in all_executions.columns else 'id'
                total_executed = all_executions.groupby(id_column)['실제집행수'].sum()
                influencer_summary['전체_집행수'] = influencer_summary['id'].map(total_executed).fillna(0).astype(int)
                
                # 전체 잔여수 = 전체 계약수 - 전체 집행수
                influencer_summary['전체_잔여수'] = influencer_summary['전체_계약수'] - influencer_summary['전체_집행수']
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
    
    # 집행완료된 배정만 월별 브랜드 정보로 표시
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
                influencer_id = row["ID"]
                for month in months:
                    # 해당 인플루언서의 해당 월 집행 내역
                    month_executions = completed_executions[
                        (completed_executions['id'] == influencer_id) & 
                        (completed_executions['배정월'] == month)
                    ]
                    
                    if not month_executions.empty:
                        # 브랜드별로 고정 순서로 표시 (MLB,DX,DV,ST)
                        brands = month_executions['브랜드'].unique()
                        # 고정 순서로 정렬
                        brand_order = ["MLB", "DX", "DV", "ST"]
                        sorted_brands = [brand for brand in brand_order if brand in brands]
                        influencer_summary.loc[influencer_summary['id'] == influencer_id, month] = ", ".join(sorted_brands)


def render_influencer_table(influencer_summary, selected_brand_filter, influencer_count=None):
    """인플루언서 테이블 렌더링"""
    # 브랜드 하이라이트 CSS 추가 (전체 필터일 때도 동일한 CSS 구조 유지)
    if selected_brand_filter != "전체":
        st.markdown(f"""
        <style>
        .stDataFrame [data-testid="stDataFrameCell"]:has-text("{selected_brand_filter}") {{
            background-color: #e3f2fd !important;
            color: #1976d2 !important;
            font-weight: bold !important;
        }}
        </style>
        """, unsafe_allow_html=True)
    else:
        # 전체 필터일 때도 동일한 CSS 구조 유지 (하이라이트 없음)
        st.markdown("""
        <style>
        /* 전체 필터일 때는 하이라이트 없음 */
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
    
    # 다운로드 버튼
    st.download_button(
        "📥 인플루언서별 배정 현황 엑셀 다운로드",
        to_excel_bytes(influencer_summary),
        file_name="influencer_summary.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="influencer_excel_download_button"
    )

def get_influencer_column_config():
    """인플루언서 컬럼 설정"""
    return {
        "번호": st.column_config.NumberColumn(
            "번호",
            help="순서 번호",
            format="%d"
        ),
        "ID": st.column_config.TextColumn(
            "ID",
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
            help="9월 배정 브랜드",
            max_chars=None
        ),
        "10월": st.column_config.TextColumn(
            "10월",
            help="10월 배정 브랜드",
            max_chars=None
        ),
        "11월": st.column_config.TextColumn(
            "11월",
            help="11월 배정 브랜드",
            max_chars=None
        ),
        "12월": st.column_config.TextColumn(
            "12월",
            help="12월 배정 브랜드",
            max_chars=None
        ),
        "1월": st.column_config.TextColumn(
            "1월",
            help="1월 배정 브랜드",
            max_chars=None
        ),
        "2월": st.column_config.TextColumn(
            "2월",
            help="2월 배정 브랜드",
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
            st.rerun()

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

def main():
    # 페이지 설정
    st.set_page_config(page_title="인플루언서 배정 앱", layout="wide")
    load_css()
    
    st.title("🎯 인플루언서 배정 앱")
    
    # 앱 시작 시 GitHub에서 최신 데이터 가져오기 (조용히)
    if 'data_synced' not in st.session_state:
        with st.spinner("🔄 GitHub에서 최신 데이터를 가져오는 중..."):
            # 조용히 데이터 가져오기 (알림 없이)
            try:
                result = subprocess.run(['git', 'pull', 'origin', 'master'], 
                                      capture_output=True, text=True, cwd=SCRIPT_DIR)
            except Exception as e:
                pass  # 오류가 있어도 조용히 처리
        st.session_state.data_synced = True
    
    # 새로고침 시 전체 선택 상태 초기화
    st.session_state.select_all = False
    
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
    tab1, tab2 = st.tabs(["📊 배정 및 집행결과", "👥 인플루언서별"])
    
    # 현재 탭 상태 업데이트
    if tab1:
        st.session_state.current_tab = 0
    elif tab2:
        st.session_state.current_tab = 1
    
    with tab1:
        render_assignment_results_tab(month_options, df)
    
    with tab2:
        render_influencer_tab(df)

if __name__ == "__main__":
    main()