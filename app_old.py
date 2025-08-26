import streamlit as st
import pandas as pd
import os
from datetime import datetime
import io

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
SEASON_OPTIONS = ["25FW", "26SS"]
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
    return FW_MONTHS if season == "25FW" else SS_MONTHS

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
            
            if not execution_data.empty:
                for _, assignment in previous_month_assignments.iterrows():
                    exec_mask = (
                        (execution_data['ID'] == assignment['ID']) &
                        (execution_data['브랜드'] == assignment['브랜드']) &
                        (execution_data['배정월'] == assignment['배정월'])
                    )
                    
                    if not exec_mask.any() or execution_data.loc[exec_mask, '실제집행수'].iloc[0] == 0:
                        incomplete_assignments.append(f"{assignment['이름']} ({assignment['브랜드']})")
            
            if incomplete_assignments:
                return False, incomplete_assignments, previous_month
    
    return True, [], None

def display_incomplete_assignments(incomplete_assignments, previous_month, df):
    """미완료 배정 목록 표시"""
    st.error(f"❌ {previous_month} 배정된 인플루언서 중 결과가 업데이트되지 않은 배정이 있습니다. 모든 이전 달 결과가 업데이트 된 상태여야 다음 달 배정이 가능합니다.")
    
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
    already_assigned_influencers = set(selected_month_assignments["ID"].unique()) if not selected_month_assignments.empty else set()
    
    if already_assigned_influencers:
        st.info(f"ℹ️ {selected_month}에 이미 배정된 인플루언서가 있습니다. 기존 배정에 추가로 배정합니다.")
    
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
                
                # 배정 정보 생성
                assignment_info = create_assignment_info(row, brand, selected_month, df)
                results.append(assignment_info)
                
                newly_assigned_influencers.add(row["id"])
                assigned_count += 1
    
    # 결과 저장
    if results:
        save_assignments(results, existing_history)
        st.success(f"✅ {selected_month} 배정이 완료되었습니다!")
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
    
    if not execution_data.empty:
        exec_mask = (
            (execution_data['ID'] == row['id']) &
            (execution_data['브랜드'] == brand)
        )
        if exec_mask.any():
            brand_execution_count = execution_data.loc[exec_mask, '실제집행수'].sum()
        
        total_exec_mask = (execution_data['ID'] == row['id'])
        if total_exec_mask.any():
            total_execution_count = execution_data.loc[total_exec_mask, '실제집행수'].sum()
    
    # 잔여수 계산
    brand_remaining = max(0, original_brand_qty - brand_execution_count)
    total_remaining = max(0, original_total_qty - total_execution_count)
    
    return {
        "브랜드": brand,
        "ID": row["id"],
        "이름": row["name"],
        "배정월": selected_month,
        "FLW": row["follower"],
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
    
    if not existing_history.empty:
        updated_history = pd.concat([existing_history, result_df], ignore_index=True)
    else:
        updated_history = result_df
    
    updated_history.to_csv(ASSIGNMENT_FILE, index=False, encoding="utf-8")

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
        
        # 중복 배정 확인
        existing_mask = (
            (assignment_history['ID'] == influencer_id) &
            (assignment_history['배정월'] == selected_month)
        )
        
        if not existing_mask.any():
            # 새로운 배정 추가
            new_assignment = create_manual_assignment_info(influencer_id, brand, selected_month, df)
            assignment_history = pd.concat([assignment_history, pd.DataFrame([new_assignment])], ignore_index=True)
            assignment_history.to_csv(ASSIGNMENT_FILE, index=False, encoding="utf-8")
            
            st.sidebar.success(f"✅ {influencer_name} 배정 추가됨!")
            
            if 'selected_id' in st.session_state:
                st.session_state.selected_id = ""
            
            st.rerun()
        else:
            st.sidebar.warning(f"⚠️ {influencer_name}의 {selected_month} 배정이 이미 존재합니다.")
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
        'ID': influencer_id,
        '이름': influencer_data['name'],
        '배정월': selected_month,
        'FLW': influencer_data['follower'],
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
        info_container = st.sidebar.container()
        with info_container:
            col1, col2 = st.columns([20, 1])
            with col1:
                st.sidebar.success(f"✅ {st.session_state.selected_id} 선택됨!")
            with col2:
                if st.sidebar.button("✕", key="close_selected_id_info", help="닫기"):
                    st.session_state.selected_id = ""
                    st.rerun()

def render_assignment_results_tab(month_options, df):
    """배정 및 집행결과 탭 렌더링"""
    st.subheader("📊 배정 및 집행결과")
    
    # 필터
    selected_month_filter = st.selectbox("📅 월별 필터", month_options, index=0, key="tab1_month_filter")
    selected_brand_filter = st.selectbox("🏷️ 브랜드 필터", BRAND_OPTIONS, index=0, key="tab1_brand_filter")
    
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
    
    # 컬럼 순서 재정렬
    cols = ['선택', '번호', '배정월', '브랜드', 'ID', '이름', 'FLW', '결과', '집행URL'] + [col for col in all_results_with_checkbox.columns if col not in ['선택', '번호', '배정월', '브랜드', 'ID', '이름', 'FLW', '결과', '집행URL']]
    all_results_with_checkbox = all_results_with_checkbox[cols]
    
    return all_results_with_checkbox

def update_execution_status(all_results_with_checkbox):
    """실행 상태 업데이트"""
    if os.path.exists(EXECUTION_FILE):
        execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
        if not execution_data.empty:
            for idx, row in all_results_with_checkbox.iterrows():
                exec_mask = (
                    (execution_data['ID'] == row['ID']) &
                    (execution_data['브랜드'] == row['브랜드']) &
                    (execution_data['배정월'] == row['배정월'])
                )
                if exec_mask.any() and execution_data.loc[exec_mask, '실제집행수'].iloc[0] > 0:
                    all_results_with_checkbox.loc[idx, '결과'] = '✅ 집행완료'

def process_numeric_columns(all_results_with_checkbox):
    """숫자 컬럼 처리"""
    numeric_columns = ['브랜드_계약수', '브랜드_실집행수', '브랜드_잔여수', '전체_계약수', '전체_실집행수', '전체_잔여수']
    for col in numeric_columns:
        if col in all_results_with_checkbox.columns:
            all_results_with_checkbox[col] = all_results_with_checkbox[col].fillna(0).astype(int).astype(str)
    
    if 'FLW' in all_results_with_checkbox.columns:
        all_results_with_checkbox['FLW'] = all_results_with_checkbox['FLW'].fillna(0).astype(int)

def add_execution_url_column(all_results_with_checkbox):
    """집행URL 컬럼 추가"""
    all_results_with_checkbox['집행URL'] = ""
    
    if os.path.exists(ASSIGNMENT_FILE):
        assignment_history = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        if '집행URL' in assignment_history.columns:
            for idx, row in all_results_with_checkbox.iterrows():
                url_mask = (
                    (assignment_history['ID'] == row['ID']) &
                    (assignment_history['브랜드'] == row['브랜드']) &
                    (assignment_history['배정월'] == row['배정월'])
                )
                if url_mask.any():
                    url_value = assignment_history.loc[url_mask, '집행URL'].iloc[0]
                    if pd.notna(url_value) and url_value != "":
                        all_results_with_checkbox.loc[idx, '집행URL'] = url_value

def render_table_controls(all_results_with_checkbox):
    """테이블 컨트롤 렌더링"""
    col1, col2 = st.columns([1, 4])
    
    with col1:
        if st.button("☑️ 전체선택", type="secondary", use_container_width=True):
            if 'select_all' not in st.session_state:
                st.session_state.select_all = True
            else:
                st.session_state.select_all = not st.session_state.select_all
            st.rerun()
    
    with col2:
        render_download_button(all_results_with_checkbox)

def render_download_button(all_results_with_checkbox):
    """다운로드 버튼 렌더링"""
    available_columns = ['브랜드', 'ID', '이름', '배정월', 'FLW', '결과', '집행URL', '브랜드_계약수', '브랜드_실집행수', '브랜드_잔여수', '전체_계약수', '전체_잔여수']
    existing_columns = [col for col in available_columns if col in all_results_with_checkbox.columns]
    download_data = all_results_with_checkbox[existing_columns].copy()
    
    if '결과' in download_data.columns:
        download_data['결과'] = download_data['결과'].replace({
            '📋 배정완료': '배정완료',
            '✅ 집행완료': '집행완료'
        })
    
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"assignment_results_{current_time}.xlsx"
    st.download_button(
        "📥 배정 및 집행결과 엑셀 다운로드",
        to_excel_bytes(download_data),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def render_data_editor(all_results_with_checkbox):
    """데이터 에디터 렌더링"""
    return st.data_editor(
        all_results_with_checkbox,
        use_container_width=True,
        hide_index=True,
        key="assignment_data_editor",
        column_config={
            "선택": st.column_config.CheckboxColumn(
                "선택",
                help="실집행완료할 배정을 선택하세요",
                default=False,
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
            "브랜드_계약수": st.column_config.TextColumn(
                "브랜드_계약수",
                help="브랜드별 계약 수",
                max_chars=None
            ),
            "브랜드_실집행수": st.column_config.TextColumn(
                "브랜드_실집행수",
                help="브랜드별 실제 집행 수",
                max_chars=None
            ),
            "브랜드_잔여수": st.column_config.TextColumn(
                "브랜드_잔여수",
                help="브랜드별 잔여 수",
                max_chars=None
            ),
            "전체_계약수": st.column_config.TextColumn(
                "전체_계약수",
                help="전체 계약 수",
                max_chars=None
            ),
            "전체_실집행수": st.column_config.TextColumn(
                "전체_실집행수",
                help="전체 실제 집행 수",
                max_chars=None
            ),
            "전체_잔여수": st.column_config.TextColumn(
                "전체_잔여수",
                help="전체 잔여 수",
                max_chars=None
            )
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
                'ID': row['ID'],
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
                'ID': row['ID'],
                '이름': row['이름'],
                '브랜드': row['브랜드'],
                '배정월': row['배정월']
            })
        elif original_result == '✅ 집행완료' and new_result == '📋 배정완료':
            changed_to_assigned.append({
                'ID': row['ID'],
                '이름': row['이름'],
                '브랜드': row['브랜드'],
                '배정월': row['배정월']
            })
    
    if changed_to_executed:
        update_execution_data(changed_to_executed, add=True)
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
        assignment_history = pd.DataFrame(columns=["브랜드", "ID", "이름", "배정월", "집행URL"])
    
    for change in url_changes:
        mask = (
            (assignment_history['ID'] == change['ID']) &
            (assignment_history['브랜드'] == change['브랜드']) &
            (assignment_history['배정월'] == change['배정월'])
        )
        if mask.any():
            assignment_history.loc[mask, '집행URL'] = change['집행URL']
    
    assignment_history.to_csv(ASSIGNMENT_FILE, index=False, encoding="utf-8")

def update_execution_data(changes, add=True):
    """실행 데이터 업데이트"""
    if os.path.exists(EXECUTION_FILE):
        execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
    else:
        execution_data = pd.DataFrame(columns=["ID", "이름", "브랜드", "배정월", "실제집행수"])
    
    for change in changes:
        existing_mask = (
            (execution_data['ID'] == change['ID']) &
            (execution_data['브랜드'] == change['브랜드']) &
            (execution_data['배정월'] == change['배정월'])
        )
        
        if add:
            if existing_mask.any():
                execution_data.loc[existing_mask, '실제집행수'] = 1
            else:
                new_row = {**change, '실제집행수': 1}
                execution_data = pd.concat([execution_data, pd.DataFrame([new_row])], ignore_index=True)
        else:
            execution_data = execution_data[~existing_mask]
    
    execution_data.to_csv(EXECUTION_FILE, index=False, encoding="utf-8")

def render_assignment_buttons(edited_df, df):
    """배정 버튼들 렌더링"""
    col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
    
    with col1:
        render_execution_complete_button(edited_df)
    
    with col2:
        render_delete_assignment_button(edited_df, df)
    
    with col3:
        render_reset_assignment_button(df)

def render_execution_complete_button(edited_df):
    """집행완료 버튼 렌더링"""
    if st.button("✅ 집행완료", type="secondary", use_container_width=True):
        selected_rows = edited_df[edited_df['선택'] == True]
        
        if not selected_rows.empty:
            changes = []
            for _, row in selected_rows.iterrows():
                changes.append({
                    'ID': row['ID'],
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
        selected_rows = edited_df[edited_df['선택'] == True]
        
        if not selected_rows.empty:
            execution_completed_selected = []
            deletable_rows = []
            
            for _, row in selected_rows.iterrows():
                if is_execution_completed(row):
                    execution_completed_selected.append(f"{row['이름']} ({row['브랜드']})")
                else:
                    deletable_rows.append(row)
            
            if execution_completed_selected:
                create_warning_container("집행완료 상태의 배정이 있어 삭제할 수 없습니다. 집행완료를 배정완료로 변경한 후 다시 시도해주세요.", "close_delete_warning")
            
            if deletable_rows:
                delete_assignments(deletable_rows)
                st.success(f"✅ {len(deletable_rows)}개의 배정이 삭제되었습니다!")
                st.rerun()
        else:
            st.warning("⚠️ 삭제할 배정을 선택해주세요.")

def render_reset_assignment_button(df):
    """배정초기화 버튼 렌더링"""
    if st.button("🗑️ 배정초기화", type="secondary", use_container_width=True):
        execution_completed_assignments = get_execution_completed_assignments()
        
        if execution_completed_assignments:
            create_warning_container("집행완료 상태의 배정이 있어 초기화할 수 없습니다. 집행완료를 배정완료로 변경한 후 다시 시도해주세요.", "close_init_warning")
        else:
            reset_assignments()
            create_success_container("✅ 초기화가 완료되었습니다!", "close_init_success")
            st.rerun()

def is_execution_completed(row):
    """집행완료 상태인지 확인"""
    if os.path.exists(EXECUTION_FILE):
        execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
        if not execution_data.empty:
            exec_mask = (
                (execution_data['ID'] == row['ID']) &
                (execution_data['브랜드'] == row['브랜드']) &
                (execution_data['배정월'] == row['배정월'])
            )
            return exec_mask.any() and execution_data.loc[exec_mask, '실제집행수'].iloc[0] > 0
    return False

def get_execution_completed_assignments():
    """집행완료된 배정 목록 가져오기"""
    execution_completed_assignments = []
    assignment_history = load_assignment_history()
    
    if os.path.exists(EXECUTION_FILE):
        execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
        if not execution_data.empty:
            for _, row in assignment_history.iterrows():
                exec_mask = (
                    (execution_data['ID'] == row['ID']) &
                    (execution_data['브랜드'] == row['브랜드']) &
                    (execution_data['배정월'] == row['배정월'])
                )
                if exec_mask.any() and execution_data.loc[exec_mask, '실제집행수'].iloc[0] > 0:
                    execution_completed_assignments.append(f"{row['이름']} ({row['브랜드']})")
    
    return execution_completed_assignments

def delete_assignments(deletable_rows):
    """배정 삭제"""
    assignment_history = load_assignment_history()
    rows_to_remove = []
    
    for row in deletable_rows:
        mask = (
            (assignment_history['브랜드'] == row['브랜드']) &
            (assignment_history['ID'] == row['ID']) &
            (assignment_history['배정월'] == row['배정월'])
        )
        rows_to_remove.extend(assignment_history[mask].index.tolist())
    
    rows_to_remove = list(set(rows_to_remove))
    assignment_history = assignment_history.drop(rows_to_remove)
    assignment_history.to_csv(ASSIGNMENT_FILE, index=False, encoding="utf-8")

def reset_assignments():
    """배정 초기화"""
    if os.path.exists(ASSIGNMENT_FILE):
        os.remove(ASSIGNMENT_FILE)
    if os.path.exists(EXECUTION_FILE):
        os.remove(EXECUTION_FILE)

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
        
        required_columns = ['브랜드', 'ID', '이름', '배정월', 'FLW', '브랜드_계약수', '브랜드_실집행수', '브랜드_잔여수', '전체_계약수', '전체_실집행수', '전체_잔여수', '집행URL']
        missing_columns = [col for col in required_columns if col not in uploaded_data.columns]
        
        if missing_columns:
            st.error(f"❌ 필수 컬럼이 누락되었습니다: {', '.join(missing_columns)}")
        else:
            process_uploaded_data(uploaded_data, df)
            
    except Exception as e:
        st.error(f"❌ 파일 업로드 중 오류가 발생했습니다: {str(e)}")

def process_uploaded_data(uploaded_data, df):
    """업로드된 데이터 처리"""
    uploaded_data = uploaded_data[required_columns].copy()
    
    # 배정 이력 업데이트
    assignment_update_data = uploaded_data[['브랜드', 'ID', '이름', '배정월', '집행URL']].copy()
    update_assignment_history(assignment_update_data)
    
    # 실집행수 데이터 업데이트
    execution_update_data = uploaded_data[uploaded_data['브랜드_실집행수'] > 0][['ID', '브랜드', '배정월', '브랜드_실집행수']].copy()
    execution_update_data = execution_update_data.rename(columns={'브랜드_실집행수': '실제집행수'})
    execution_update_data = execution_update_data.merge(
        df[['id', 'name']].rename(columns={'id': 'ID', 'name': '이름'}),
        on='ID',
        how='left'
    )
    update_execution_history(execution_update_data)
    
    st.success(f"✅ {len(assignment_update_data)}개의 배정 데이터와 {len(execution_update_data)}개의 실집행수 데이터가 업로드되었습니다!")
    
    # 미리보기
    st.markdown("**업로드된 배정 데이터 미리보기:**")
    st.dataframe(assignment_update_data, use_container_width=True)
    
    if not execution_update_data.empty:
        st.markdown("**업로드된 실집행수 데이터 미리보기:**")
        st.dataframe(execution_update_data, use_container_width=True)
    
    st.rerun()

def update_assignment_history(assignment_update_data):
    """배정 이력 업데이트"""
    if os.path.exists(ASSIGNMENT_FILE):
        existing_assignment_data = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        if '집행URL' not in existing_assignment_data.columns:
            existing_assignment_data['집행URL'] = ""
    else:
        existing_assignment_data = pd.DataFrame(columns=["브랜드", "ID", "이름", "배정월", "집행URL"])
    
    combined_assignment_data = pd.concat([existing_assignment_data, assignment_update_data], ignore_index=True)
    combined_assignment_data = combined_assignment_data.drop_duplicates(subset=['ID', '브랜드', '배정월'], keep='last')
    combined_assignment_data.to_csv(ASSIGNMENT_FILE, index=False, encoding="utf-8")

def update_execution_history(execution_update_data):
    """실행 이력 업데이트"""
    if os.path.exists(EXECUTION_FILE):
        existing_execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
    else:
        existing_execution_data = pd.DataFrame(columns=["ID", "이름", "브랜드", "배정월", "실제집행수"])
    
    combined_execution_data = pd.concat([existing_execution_data, execution_update_data], ignore_index=True)
    combined_execution_data = combined_execution_data.drop_duplicates(subset=['ID', '브랜드', '배정월'], keep='last')
    combined_execution_data.to_csv(EXECUTION_FILE, index=False, encoding="utf-8")

def render_influencer_tab(df):
    """인플루언서별 탭 렌더링"""
    st.subheader("👥 인플루언서별 배정 현황")
    
    # 브랜드 필터
    selected_brand_filter = st.selectbox("🏷️ 브랜드 필터", BRAND_OPTIONS, index=0, key="tab2_brand_filter")
    
    # 인플루언서 요약 데이터 준비
    influencer_summary = prepare_influencer_summary(df, selected_brand_filter)
    
    if not influencer_summary.empty:
        render_influencer_table(influencer_summary, selected_brand_filter)
    else:
        st.info("인플루언서 데이터가 없습니다.")

def prepare_influencer_summary(df, selected_brand_filter):
    """인플루언서 요약 데이터 준비"""
    influencer_summary = df[["id", "name", "follower", "unit_fee"]].copy()
    
    # 전체 계약수 계산
    qty_cols = [f"{brand.lower()}_qty" for brand in BRANDS]
    influencer_summary["전체_계약수"] = df.loc[influencer_summary.index, qty_cols].sum(axis=1)
    
    # 브랜드 필터 적용
    if selected_brand_filter != "전체":
        qty_col = f"{selected_brand_filter.lower()}_qty"
        if qty_col in df.columns:
            brand_filter_mask = df[qty_col] > 0
            influencer_summary = influencer_summary[brand_filter_mask]
    
    # 브랜드별 상세 정보 추가
    add_brand_details(influencer_summary, df, selected_brand_filter)
    
    # 월별 컬럼 추가
    add_monthly_columns(influencer_summary, df)
    
    # 번호 컬럼 추가
    influencer_summary = influencer_summary.reset_index(drop=True)
    influencer_summary.insert(0, '번호', range(1, len(influencer_summary) + 1))
    
    # 컬럼명 변경
    influencer_summary = influencer_summary.rename(columns={
        "id": "ID", "name": "이름", "follower": "FLW", "unit_fee": "1회계약단가"
    })
    
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
        
        # 실집행수 추가
        if os.path.exists(EXECUTION_FILE):
            execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
            if not execution_data.empty:
                brand_exec_counts = execution_data[execution_data["브랜드"] == selected_brand].groupby("ID")["실제집행수"].sum()
                influencer_summary[f"{selected_brand}_실집행수"] = influencer_summary["id"].map(brand_exec_counts).fillna(0).astype(int)
            else:
                influencer_summary[f"{selected_brand}_실집행수"] = 0
        else:
            influencer_summary[f"{selected_brand}_실집행수"] = 0
        
        # 잔여횟수 계산
        max_qty = influencer_summary[f"{selected_brand}_계약수"]
        influencer_summary[f"{selected_brand}_잔여횟수"] = max_qty - influencer_summary[f"{selected_brand}_실집행수"]
        influencer_summary[f"{selected_brand}_잔여횟수"] = influencer_summary[f"{selected_brand}_잔여횟수"].clip(lower=0)
    else:
        # 전체 선택 시 모든 브랜드 잔여횟수 표시
        for brand in BRANDS:
            qty_col = f"{brand.lower()}_qty"
            if qty_col in df.columns:
                max_qty = df.loc[influencer_summary.index, qty_col]
            else:
                max_qty = pd.Series(0, index=influencer_summary.index)
            
            if os.path.exists(EXECUTION_FILE):
                execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
                if not execution_data.empty:
                    brand_exec_counts = execution_data[execution_data["브랜드"] == brand].groupby("ID")["실제집행수"].sum()
                    influencer_summary[f"잔여횟수_{brand}"] = influencer_summary.apply(
                        lambda row: max(0, max_qty.get(row.name, 0) - brand_exec_counts.get(row["id"], 0)), axis=1
                    )
                else:
                    influencer_summary[f"잔여횟수_{brand}"] = max_qty
            else:
                influencer_summary[f"잔여횟수_{brand}"] = max_qty

def add_monthly_columns(influencer_summary, df):
    """월별 컬럼 추가"""
    months = ["9월", "10월", "11월", "12월", "1월", "2월"]
    for month in months:
        influencer_summary[month] = ""
    
    # 배정 이력에서 월별 브랜드 정보 채우기
    if os.path.exists(ASSIGNMENT_FILE):
        assignment_history = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        for _, row in assignment_history.iterrows():
            influencer_id = row["ID"]
            month = row["배정월"]
            brand = row["브랜드"]
            mask = influencer_summary["id"] == influencer_id
            
            if mask.any() and month in influencer_summary.columns:
                is_executed = False
                if os.path.exists(EXECUTION_FILE):
                    execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
                    if not execution_data.empty:
                        exec_mask = (
                            (execution_data['ID'] == influencer_id) &
                            (execution_data['브랜드'] == brand) &
                            (execution_data['배정월'] == month)
                        )
                        if exec_mask.any() and execution_data.loc[exec_mask, '실제집행수'].iloc[0] > 0:
                            is_executed = True
                
                if is_executed:
                    current_value = influencer_summary.loc[mask, month].iloc[0]
                    if current_value == "":
                        influencer_summary.loc[mask, month] = brand
                    else:
                        influencer_summary.loc[mask, month] = current_value + ", " + brand

def render_influencer_table(influencer_summary, selected_brand_filter):
    """인플루언서 테이블 렌더링"""
    # 브랜드 하이라이트 CSS 추가
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
    
    # 편집 가능한 데이터프레임으로 표시
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
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def get_influencer_column_config():
    """인플루언서 컬럼 설정"""
    return {
        "번호": st.column_config.NumberColumn(
            "번호",
            help="순서 번호",
            format="%d",
            width=10
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
            help="1회 계약 단가",
            format="%d",
            step=1
        ),
        "전체계약횟수": st.column_config.NumberColumn(
            "전체계약횟수",
            help="전체 계약 횟수",
            format="%d",
            step=1
        ),
        "실집행수": st.column_config.NumberColumn(
            "실집행수",
            help="실제 집행 수",
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
        "9월": st.column_config.SelectboxColumn(
            "9월",
            help="9월 배정 브랜드",
            width="small",
            options=["", "MLB", "DX", "DV", "ST"]
        ),
        "10월": st.column_config.SelectboxColumn(
            "10월",
            help="10월 배정 브랜드",
            width="small",
            options=["", "MLB", "DX", "DV", "ST"]
        ),
        "11월": st.column_config.SelectboxColumn(
            "11월",
            help="11월 배정 브랜드",
            width="small",
            options=["", "MLB", "DX", "DV", "ST"]
        ),
        "12월": st.column_config.SelectboxColumn(
            "12월",
            help="12월 배정 브랜드",
            width="small",
            options=["", "MLB", "DX", "DV", "ST"]
        ),
        "1월": st.column_config.SelectboxColumn(
            "1월",
            help="1월 배정 브랜드",
            width="small",
            options=["", "MLB", "DX", "DV", "ST"]
        ),
        "2월": st.column_config.SelectboxColumn(
            "2월",
            help="2월 배정 브랜드",
            width="small",
            options=["", "MLB", "DX", "DV", "ST"]
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
            if pd.notna(row['ID']) and row['ID'] != "":
                for month in months:
                    new_value = row[month]
                    if new_value and new_value != "":
                        existing_mask = (
                            (assignment_history['ID'] == row['ID']) &
                            (assignment_history['배정월'] == month)
                        )
                        
                        if not existing_mask.any():
                            new_assignments.append({
                                '브랜드': new_value,
                                'ID': row['ID'],
                                '이름': row['이름'],
                                '배정월': month
                            })
                        else:
                            existing_brand = assignment_history.loc[existing_mask, '브랜드'].iloc[0]
                            if new_value != existing_brand:
                                assignment_history.loc[existing_mask, '브랜드'] = new_value
                                updated_assignments.append({
                                    '브랜드': new_value,
                                    'ID': row['ID'],
                                    '이름': row['이름'],
                                    '배정월': month
                                })
        
        if new_assignments or updated_assignments:
            assignment_history.to_csv(ASSIGNMENT_FILE, index=False, encoding="utf-8")
            
            if new_assignments and updated_assignments:
                message = f"✅ {len(new_assignments)}개의 새로운 배정이 추가되고 {len(updated_assignments)}개의 배정이 수정되었습니다!"
            elif new_assignments:
                message = f"✅ {len(new_assignments)}개의 새로운 배정이 추가되었습니다!"
            elif updated_assignments:
                message = f"✅ {len(updated_assignments)}개의 배정이 수정되었습니다!"
            
            create_success_container(message, "close_influencer_success")
            st.rerun()

# =============================================================================
# 메인 앱
# =============================================================================

def main():
    # 페이지 설정
    st.set_page_config(page_title="인플루언서 배정 앱", layout="wide")
    load_css()
    
    st.title("🎯 인플루언서 배정 앱")
    
    # 데이터 로드
    df = load_influencer_data()
    if df is None:
        return
    
    # 사이드바 렌더링
    selected_month, selected_season, month_options = render_sidebar(df)
    
    # 탭 생성
    tab1, tab2 = st.tabs(["📊 배정 및 집행결과", "👥 인플루언서별"])
    
    with tab1:
        render_assignment_results_tab(month_options, df)
    
    with tab2:
        render_influencer_tab(df)

if __name__ == "__main__":
    main()