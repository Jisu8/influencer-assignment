import pandas as pd
import streamlit as st
import os
from datetime import datetime
from io import BytesIO

def to_excel_bytes(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def create_execution_template():
    """실제집행수 템플릿 생성 - 배정 결과 기반"""
    # 배정 이력에서 현재 배정된 인플루언서들 가져오기
    if os.path.exists(assignment_history_file):
        assignment_history = pd.read_csv(assignment_history_file, encoding="utf-8")
        if not assignment_history.empty:
            # 배정된 인플루언서들로 템플릿 생성
            template_df = assignment_history[["브랜드", "ID", "이름", "배정월"]].copy()
            template_df["계획수"] = 1
            template_df["실제집행수"] = 0  # 기본값 0
            
            # 기존 실제집행수 데이터가 있으면 병합
            if os.path.exists(execution_status_file):
                existing_execution = pd.read_csv(execution_status_file, encoding="utf-8")
                if not existing_execution.empty:
                    # 기존 데이터와 병합하여 실제집행수 유지
                    template_df = template_df.merge(
                        existing_execution[["브랜드", "ID", "이름", "배정월", "실제집행수"]], 
                        on=["브랜드", "ID", "이름", "배정월"], 
                        how="left"
                    )
                    # 병합 후 실제집행수 컬럼이 없으면 새로 생성
                    if "실제집행수" not in template_df.columns:
                        template_df["실제집행수"] = 0
                    else:
                        # 병합되지 않은 행의 실제집행수는 0으로 설정
                        template_df["실제집행수"] = template_df["실제집행수"].fillna(0)
                else:
                    template_df["실제집행수"] = 0
            else:
                template_df["실제집행수"] = 0
            
            return template_df
    
    # 배정 이력이 없을 때 기본 템플릿
    template_data = {
        "브랜드": ["MLB", "DX", "DV", "ST"],
        "ID": ["example_id_1", "example_id_2", "example_id_3", "example_id_4"],
        "이름": ["예시인플루언서1", "예시인플루언서2", "예시인플루언서3", "예시인플루언서4"],
        "배정월": ["9월", "9월", "9월", "9월"],
        "계획수": [1, 1, 1, 1],
        "실제집행수": [0, 0, 0, 0]
    }
    template_df = pd.DataFrame(template_data)
    return template_df

st.set_page_config(page_title="Influencer Assignment", layout="wide")

st.title("🎯 인플루언서 배정 앱")

# CSV 데이터 로드
df = pd.read_csv("data/influencer.csv", encoding="utf-8", sep=",")
df.columns = df.columns.str.strip()

# 배정 이력 파일 경로
assignment_history_file = "data/assignment_history.csv"
execution_status_file = "data/execution_status.csv"

# 배정 이력 로드 (파일이 없으면 새로 생성)
if os.path.exists(assignment_history_file):
    assignment_history = pd.read_csv(assignment_history_file, encoding="utf-8")
else:
    assignment_history = pd.DataFrame()

# 실제 집행수 로드 (파일이 없으면 새로 생성)
if os.path.exists(execution_status_file):
    execution_status = pd.read_csv(execution_status_file, encoding="utf-8")
else:
    execution_status = pd.DataFrame(columns=["브랜드", "ID", "이름", "배정월", "계획수", "실제집행수"])

# 시즌 선택
st.sidebar.header("🌤️ 시즌 선택")
season = st.sidebar.selectbox(
    "시즌을 선택하세요",
    options=["25FW", "26SS"],
    index=0
)

# 시즌에 따른 월 선택
if season == "25FW":
    month_options = ["9월", "10월", "11월", "12월", "1월", "2월"]
    default_index = 0
else:  # 26SS
    month_options = ["3월", "4월", "5월", "6월", "7월", "8월"]
    default_index = 0

# 배정 월 입력
st.sidebar.header("📅 배정 월")
assignment_month = st.sidebar.selectbox(
    "배정 월을 선택하세요",
    options=month_options,
    index=default_index
)

# 브랜드별 수량 입력 헤더와 입력 필드
st.sidebar.header("📌 브랜드별 배정 수량")
mlb = st.sidebar.number_input("MLB", min_value=0, max_value=100, value=0)
dx = st.sidebar.number_input("DX", min_value=0, max_value=100, value=0)
dv = st.sidebar.number_input("DV", min_value=0, max_value=100, value=0)
stb = st.sidebar.number_input("ST", min_value=0, max_value=100, value=0)

assignments = {"mlb_qty": mlb, "dx_qty": dx, "dv_qty": dv, "st_qty": stb}
brand_map = {
    "mlb_qty": "MLB",
    "dx_qty": "DX",
    "dv_qty": "DV",
    "st_qty": "ST"
}

# 실행 버튼
if st.sidebar.button("🚀 배정 실행"):
    results = []
    df_copy = df.copy()
    
    # 선택된 월에 이미 배정된 인플루언서 ID 가져오기
    if not assignment_history.empty:
        already_assigned_this_month = assignment_history[
            assignment_history["배정월"] == assignment_month
        ]["ID"].tolist()
    else:
        already_assigned_this_month = []
    
    # 실제 집행수에 따른 잔여수량 조정
    if not execution_status.empty:
        # 실제 집행수가 있는 인플루언서들의 집행수 합계
        executed_counts = execution_status.groupby("ID")["실제집행수"].sum()
        
        # df_copy의 잔여수량을 실제 집행수만큼 차감
        for influencer_id, executed_count in executed_counts.items():
            if influencer_id in df_copy["id"].values:
                # 해당 인플루언서의 모든 브랜드 잔여수량 차감
                for brand in ["mlb_qty", "dx_qty", "dv_qty", "st_qty"]:
                    if brand in df_copy.columns:
                        current_qty = df_copy.loc[df_copy["id"] == influencer_id, brand].iloc[0]
                        df_copy.loc[df_copy["id"] == influencer_id, brand] = max(0, current_qty - executed_count)
                
                # 전체 잔여수량도 차감
                if "total_qty" in df_copy.columns:
                    current_total = df_copy.loc[df_copy["id"] == influencer_id, "total_qty"].iloc[0]
                    df_copy.loc[df_copy["id"] == influencer_id, "total_qty"] = max(0, current_total - executed_count)

    already_assigned_this_month = set()
    for col, count in assignments.items():
        # 이미 배정된 인플루언서 제외 (같은 실행 내 + 같은 월에 이미 배정된 인플루언서)
        available = df_copy[
            (df_copy[col] > 0) & 
            (~df_copy["id"].isin(already_assigned_this_month))
        ]

        selected = available.head(count)

        # 배정된 인플루언서 ID를 누적
        already_assigned_this_month.update(selected["id"].tolist())

        for _, row in selected.iterrows():
            results.append({
                "브랜드": brand_map[col],
                "ID": row["id"],
                "이름": row["name"],
                "FLW": row["follower"],
                "브랜드_계약수": row[col],
                "브랜드_실집행수": 0,  # 기본값 0
                "브랜드_잔여수": row[col] - 1,
                "전체_계약수": row["total_qty"],
                "전체_실집행수": 0,  # 기본값 0
                "전체_잔여수": row["total_qty"] - 1,
                "배정월": assignment_month
            })

            # 계약 수량 차감
            df_copy.loc[df_copy["id"] == row["id"], col] -= 1
            df_copy.loc[df_copy["id"] == row["id"], "total_qty"] -= 1

    # assignment_history에서 현재 배정 월의 기록은 삭제
    if os.path.exists(assignment_history_file):
        assignment_history = pd.read_csv(assignment_history_file, encoding="utf-8")
        assignment_history = assignment_history[assignment_history["배정월"] != assignment_month]
    else:
        assignment_history = pd.DataFrame()
    
    # 이번 배정 결과를 assignment_history에 추가
    new_assignments = pd.DataFrame(results)
    assignment_history = pd.concat([assignment_history, new_assignments], ignore_index=True)
    assignment_history.to_csv(assignment_history_file, index=False, encoding="utf-8")
    
    # 배정 결과 DataFrame 생성
    result_df = pd.DataFrame(results)
    # 실제집행수 컬럼 추가
    if os.path.exists(execution_status_file):
        execution_data = pd.read_csv(execution_status_file, encoding="utf-8")
        if not execution_data.empty:
            # 전체 실제집행수 계산
            total_execution = execution_data.groupby("ID")["실제집행수"].sum().reset_index()
            total_execution.columns = ["ID", "전체_실집행수"]
            
            # 브랜드별 실제집행수 계산
            brand_execution = execution_data.groupby(["ID", "브랜드"])["실제집행수"].sum().reset_index()
            brand_execution.columns = ["ID", "브랜드", "브랜드_실집행수"]
            
            # result_df에 실제집행수 컬럼 추가
            result_df = result_df.merge(total_execution, on="ID", how="left")
            result_df = result_df.merge(brand_execution, on=["ID", "브랜드"], how="left")
            
            # NaN 값을 0으로 채우기
            result_df["전체_실집행수"] = result_df["전체_실집행수"].fillna(0)
            result_df["브랜드_실집행수"] = result_df["브랜드_실집행수"].fillna(0)
        else:
            result_df["전체_실집행수"] = 0
            result_df["브랜드_실집행수"] = 0
    else:
        result_df["전체_실집행수"] = 0
        result_df["브랜드_실집행수"] = 0
    
    # 컬럼 순서 변경 (요청된 순서로 정렬)
    expected_columns = ["브랜드", "ID", "이름", "배정월", "FLW", "브랜드_계약수", "브랜드_실집행수", "브랜드_잔여수", "전체_계약수", "전체_실집행수", "전체_잔여수"]
    available_columns = [col for col in expected_columns if col in result_df.columns]
    if available_columns:
        result_df = result_df[available_columns]
    
    # 인플루언서별 요약 생성 및 잔여횟수 계산은 assignment_history 전체 기준으로 groupby
    influencer_summary = df[["id", "name", "follower", "unit_fee"]].copy()
    brand_list = ["MLB", "DX", "DV", "ST"]
    qty_cols = [f"{brand.lower()}_qty" for brand in brand_list]
    influencer_summary["전체계약횟수"] = df[qty_cols].sum(axis=1)
    exec_counts = assignment_history.groupby("ID").size()
    influencer_summary["전체집행횟수"] = influencer_summary["id"].map(exec_counts).fillna(0).astype(int)
    for brand in brand_list:
        qty_col = f"{brand.lower()}_qty"
        if qty_col in df.columns:
            max_qty = df[qty_col]
        else:
            max_qty = 0
        brand_counts = assignment_history[assignment_history["브랜드"] == brand].groupby("ID").size()
        influencer_summary[f"잔여횟수_{brand}"] = influencer_summary.apply(
            lambda row: max(0, max_qty.get(row.name, 0) - brand_counts.get(row["id"], 0)), axis=1
        )
    st.session_state['result_df'] = result_df
    st.session_state['influencer_summary'] = influencer_summary

    # 월별 컬럼 추가 (9월~2월)
    months = ["9월", "10월", "11월", "12월", "1월", "2월"]
    for month in months:
        st.session_state['influencer_summary'][month] = ""

    # 배정 이력에서 월별 브랜드 정보 채우기
    for _, row in assignment_history.iterrows():
        influencer_id = row["ID"]
        month = row["배정월"]
        brand = row["브랜드"]
        mask = st.session_state['influencer_summary']["id"] == influencer_id
        if mask.any() and month in st.session_state['influencer_summary'].columns:
            current_value = st.session_state['influencer_summary'].loc[mask, month].iloc[0]
            if current_value == "":
                st.session_state['influencer_summary'].loc[mask, month] = brand
            else:
                st.session_state['influencer_summary'].loc[mask, month] = current_value + ", " + brand

    # 컬럼명 변경
    st.session_state['influencer_summary'] = st.session_state['influencer_summary'].rename(columns={
        "id": "ID",
        "name": "이름", 
        "follower": "FLW",
        "unit_fee": "1회계약단가"
    })

    st.success("✅ 배정 완료!")
    # 배정 완료 후 해당 월로 이동하기 위해 session_state에 저장
    st.session_state['selected_month'] = assignment_month

# 결과 표시
if 'result_df' in st.session_state:
    result_df = st.session_state['result_df']
    influencer_summary = st.session_state['influencer_summary']
    
    # 월별 필터 추가 (전체 제거)
    months = ["9월", "10월", "11월", "12월", "1월", "2월"]
    selected_month = st.selectbox(
        "📅 월별 필터",
        options=months,
        index=0 if 'selected_month' not in st.session_state else months.index(st.session_state['selected_month'])
    )
    
    # 선택된 월에 따라 결과 필터링
    result_df = result_df[result_df["배정월"] == selected_month]
    
    # 브랜드 필터 추가
    brands = ["전체", "MLB", "DX", "DV", "ST"]
    selected_brand = st.selectbox(
        "🏷️ 브랜드 필터",
        options=brands,
        index=0
    )
    
    # 선택된 브랜드에 따라 결과 필터링
    if selected_brand != "전체":
        result_df = result_df[result_df["브랜드"] == selected_brand]
    
    tab1, tab2, tab3 = st.tabs(["📊 전체배정및집행결과", "👥 인플루언서별", "📋 실제집행수관리"])
    with tab1:
        st.subheader("📊 전체배정및집행결과")
        
        # 전체 배정 결과 표시 (필터링 없이)
        if 'result_df' in st.session_state:
            all_results = st.session_state['result_df']
        else:
            # 배정 이력에서 전체 결과 로드
            if os.path.exists(assignment_history_file):
                all_results = pd.read_csv(assignment_history_file, encoding="utf-8")
                if not all_results.empty:
                    # 실제집행수 컬럼 추가
                    if os.path.exists(execution_status_file):
                        execution_data = pd.read_csv(execution_status_file, encoding="utf-8")
                        if not execution_data.empty:
                            # 전체 실제집행수 계산
                            total_execution = execution_data.groupby("ID")["실제집행수"].sum().reset_index()
                            total_execution.columns = ["ID", "전체_실집행수"]
                            
                            # 브랜드별 실제집행수 계산
                            brand_execution = execution_data.groupby(["ID", "브랜드"])["실제집행수"].sum().reset_index()
                            brand_execution.columns = ["ID", "브랜드", "브랜드_실집행수"]
                            
                            # all_results에 실제집행수 컬럼 추가
                            all_results = all_results.merge(total_execution, on="ID", how="left")
                            all_results = all_results.merge(brand_execution, on=["ID", "브랜드"], how="left")
                            
                            # NaN 값을 0으로 채우기
                            if "전체_실집행수" in all_results.columns:
                                all_results["전체_실집행수"] = all_results["전체_실집행수"].fillna(0)
                            else:
                                all_results["전체_실집행수"] = 0
                                
                            if "브랜드_실집행수" in all_results.columns:
                                all_results["브랜드_실집행수"] = all_results["브랜드_실집행수"].fillna(0)
                            else:
                                all_results["브랜드_실집행수"] = 0
                        else:
                            all_results["전체_실집행수"] = 0
                            all_results["브랜드_실집행수"] = 0
                    else:
                        all_results["전체_실집행수"] = 0
                        all_results["브랜드_실집행수"] = 0
                else:
                    all_results = pd.DataFrame()
            else:
                all_results = pd.DataFrame()
        
        # 브랜드 필터 적용
        if selected_brand != "전체" and not all_results.empty:
            all_results = all_results[all_results["브랜드"] == selected_brand]
        
        if not all_results.empty:
            st.dataframe(all_results, use_container_width=True)
            st.download_button("📥 전체배정및집행결과 엑셀 다운로드", to_excel_bytes(all_results), file_name="all_assigned_result.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="all_assigned_result_xlsx")
        else:
            st.info("전체 배정 결과가 없습니다.")
    with tab2:
        st.subheader("👥 인플루언서별 배정 현황")
        if not influencer_summary.empty:
            st.dataframe(influencer_summary, use_container_width=True)
            st.download_button("📥 인플루언서별 배정 현황 엑셀 다운로드", to_excel_bytes(influencer_summary), file_name="influencer_summary.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="influencer_summary_xlsx")
        else:
            st.info("배정 이력이 없습니다.")
    with tab3:
        st.subheader("📋 실제집행수 관리")
        
        # 템플릿 다운로드
        st.subheader("📄 템플릿 다운로드")
        template_df = create_execution_template()
        # 현재 일시를 파일명에 추가
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        template_filename = f"execution_template_{current_time}.xlsx"
        
        st.download_button(
            "📥 실제집행수 템플릿 다운로드", 
            to_excel_bytes(template_df), 
            file_name=template_filename, 
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
            key="template_download"
        )
        
        st.info("💡 배정 결과 기반 템플릿을 다운로드하여 실제집행수를 입력한 후 업로드하세요.")
        st.info("💡 실제집행수: 0 (집행하지 않음) 또는 1 (집행함)")
        st.info("💡 이전 입력 결과가 템플릿에 자동으로 포함됩니다.")
        
        # 실제집행수 파일 업로드
        uploaded_file = st.file_uploader("실제집행수 엑셀 파일 업로드", type=['xlsx', 'xls'], key="execution_upload")
        
        if uploaded_file is not None:
            try:
                uploaded_df = pd.read_excel(uploaded_file)
                
                # 필수 컬럼 확인
                required_columns = ["브랜드", "ID", "이름", "배정월", "계획수", "실제집행수"]
                missing_columns = [col for col in required_columns if col not in uploaded_df.columns]
                
                if missing_columns:
                    st.error(f"❌ 필수 컬럼이 누락되었습니다: {', '.join(missing_columns)}")
                else:
                    st.success("✅ 파일이 성공적으로 업로드되었습니다.")
                    st.dataframe(uploaded_df, use_container_width=True)
                    
                    # 실제집행수 유효성 검사
                    invalid_execution = uploaded_df[~uploaded_df["실제집행수"].isin([0, 1])]
                    if not invalid_execution.empty:
                        st.error("❌ 실제집행수는 0 또는 1만 입력 가능합니다.")
                    else:
                        # 저장 버튼
                        if st.button("💾 실제집행수 저장", key="save_execution_2"):
                            uploaded_df.to_csv(execution_status_file, index=False, encoding="utf-8")
                            st.success("✅ 실제집행수가 성공적으로 저장되었습니다.")
                            st.rerun()
            except Exception as e:
                st.error(f"❌ 파일 읽기 오류: {str(e)}")
        
        # 현재 실제집행수 상태 표시
        st.subheader("📊 현재 실제집행수 상태")
        if os.path.exists(execution_status_file):
            current_execution = pd.read_csv(execution_status_file, encoding="utf-8")
            if not current_execution.empty:
                st.dataframe(current_execution, use_container_width=True)
                st.download_button("📥 현재 실제집행수 다운로드", to_excel_bytes(current_execution), file_name="current_execution_status.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="current_execution_download_2")
            else:
                st.info("저장된 실제집행수 데이터가 없습니다.")
        else:
            st.info("실제집행수 파일이 없습니다.")
else:
    # 배정 이력이 있으면 현재까지의 결과를 표시
    if os.path.exists(assignment_history_file):
        # 배정 이력을 다시 로드
        assignment_history_reloaded = pd.read_csv(assignment_history_file, encoding="utf-8")
    else:
        # 배정 이력 파일이 없을 때 빈 DataFrame 생성
        assignment_history_reloaded = pd.DataFrame()
    
    if not assignment_history_reloaded.empty:
        # 배정 이력에서 결과 생성
        result_df = assignment_history_reloaded.copy()
        
        # 잔여수량 계산 추가
        for _, row in result_df.iterrows():
            influencer_id = row["ID"]
            brand = row["브랜드"]
            
            # 원본 데이터에서 해당 인플루언서의 계약수량 가져오기
            if influencer_id in df["id"].values:
                influencer_data = df[df["id"] == influencer_id].iloc[0]
                
                # 브랜드별 데이터 계산
                brand_qty_col = f"{brand.lower()}_qty"
                if brand_qty_col in influencer_data:
                    original_brand_qty = influencer_data[brand_qty_col]
                    # 브랜드_계약수 추가
                    result_df.loc[result_df["ID"] == influencer_id, "브랜드_계약수"] = original_brand_qty
                    
                    # 배정된 횟수 계산
                    assigned_brand_count = len(assignment_history_reloaded[
                        (assignment_history_reloaded["ID"] == influencer_id) & 
                        (assignment_history_reloaded["브랜드"] == brand)
                    ])
                    remaining_brand_qty = max(0, original_brand_qty - assigned_brand_count)
                else:
                    result_df.loc[result_df["ID"] == influencer_id, "브랜드_계약수"] = 0
                    remaining_brand_qty = 0
                
                # 전체 데이터 계산
                original_total_qty = influencer_data["total_qty"] if "total_qty" in influencer_data else 0
                assigned_total_count = len(assignment_history_reloaded[
                    assignment_history_reloaded["ID"] == influencer_id
                ])
                remaining_total_qty = max(0, original_total_qty - assigned_total_count)
                
                # 컬럼 추가
                result_df.loc[result_df["ID"] == influencer_id, "브랜드_잔여수"] = remaining_brand_qty
                result_df.loc[result_df["ID"] == influencer_id, "전체_계약수"] = original_total_qty
                result_df.loc[result_df["ID"] == influencer_id, "전체_잔여수"] = remaining_total_qty
        
        # 실제집행수 컬럼 추가
        if os.path.exists(execution_status_file):
            execution_data = pd.read_csv(execution_status_file, encoding="utf-8")
            if not execution_data.empty:
                # 전체 실제집행수 계산
                total_execution = execution_data.groupby("ID")["실제집행수"].sum().reset_index()
                total_execution.columns = ["ID", "전체_실집행수"]
                
                # 브랜드별 실제집행수 계산
                brand_execution = execution_data.groupby(["ID", "브랜드"])["실제집행수"].sum().reset_index()
                brand_execution.columns = ["ID", "브랜드", "브랜드_실집행수"]
                
                # result_df에 실제집행수 컬럼 추가
                result_df = result_df.merge(total_execution, on="ID", how="left")
                result_df = result_df.merge(brand_execution, on=["ID", "브랜드"], how="left")
                
                # NaN 값을 0으로 채우기
                if "전체_실집행수" in result_df.columns:
                    result_df["전체_실집행수"] = result_df["전체_실집행수"].fillna(0)
                else:
                    result_df["전체_실집행수"] = 0
                    
                if "브랜드_실집행수" in result_df.columns:
                    result_df["브랜드_실집행수"] = result_df["브랜드_실집행수"].fillna(0)
                else:
                    result_df["브랜드_실집행수"] = 0
            else:
                result_df["전체_실집행수"] = 0
                result_df["브랜드_실집행수"] = 0
        else:
            result_df["전체_실집행수"] = 0
            result_df["브랜드_실집행수"] = 0
        
        # 컬럼 순서 변경 (요청된 순서로 정렬)
        expected_columns = ["브랜드", "ID", "이름", "배정월", "FLW", "브랜드_계약수", "브랜드_실집행수", "브랜드_잔여수", "전체_계약수", "전체_실집행수", "전체_잔여수"]
        available_columns = [col for col in expected_columns if col in result_df.columns]
        if available_columns:
            result_df = result_df[available_columns]
        else:
            # 기본 컬럼만 표시
            basic_columns = ["브랜드", "ID", "이름", "배정월", "FLW"]
            available_basic = [col for col in basic_columns if col in result_df.columns]
            if available_basic:
                result_df = result_df[available_basic]
    else:
        # 배정 이력이 비어있을 때 빈 DataFrame 생성
        result_df = pd.DataFrame()
    
    # 인플루언서별 요약 생성
    influencer_summary = df[["id", "name", "follower", "unit_fee"]].copy()
    brand_list = ["MLB", "DX", "DV", "ST"]
    qty_cols = [f"{brand.lower()}_qty" for brand in brand_list]
    influencer_summary["전체계약횟수"] = df[qty_cols].sum(axis=1)
    exec_counts = assignment_history_reloaded.groupby("ID").size()
    influencer_summary["전체집행횟수"] = influencer_summary["id"].map(exec_counts).fillna(0).astype(int)
    
    for brand in brand_list:
        qty_col = f"{brand.lower()}_qty"
        if qty_col in df.columns:
            max_qty = df[qty_col]
        else:
            max_qty = 0
        brand_counts = assignment_history_reloaded[assignment_history_reloaded["브랜드"] == brand].groupby("ID").size()
        influencer_summary[f"잔여횟수_{brand}"] = influencer_summary.apply(
            lambda row: max(0, max_qty.get(row.name, 0) - brand_counts.get(row["id"], 0)), axis=1
        )
    
    # 월별 컬럼 추가 (9월~2월)
    months = ["9월", "10월", "11월", "12월", "1월", "2월"]
    for month in months:
        influencer_summary[month] = ""

    # 배정 이력에서 월별 브랜드 정보 채우기
    for _, row in assignment_history_reloaded.iterrows():
        influencer_id = row["ID"]
        month = row["배정월"]
        brand = row["브랜드"]
        mask = influencer_summary["id"] == influencer_id
        if mask.any() and month in influencer_summary.columns:
            current_value = influencer_summary.loc[mask, month].iloc[0]
            if current_value == "":
                influencer_summary.loc[mask, month] = brand
            else:
                influencer_summary.loc[mask, month] = current_value + ", " + brand

    # 컬럼명 변경
    influencer_summary = influencer_summary.rename(columns={
        "id": "ID",
        "name": "이름", 
        "follower": "FLW",
        "unit_fee": "1회계약단가"
    })
    
    # 월별 필터 추가 (전체 제거)
    months = ["9월", "10월", "11월", "12월", "1월", "2월"]
    selected_month = st.selectbox(
        "📅 월별 필터",
        options=months,
        index=0
    )
    
    # 선택된 월에 따라 결과 필터링
    result_df = result_df[result_df["배정월"] == selected_month]
    
    # 브랜드 필터 추가
    brands = ["전체", "MLB", "DX", "DV", "ST"]
    selected_brand = st.selectbox(
        "🏷️ 브랜드 필터",
        options=brands,
        index=0
    )
    
    # 선택된 브랜드에 따라 결과 필터링
    if selected_brand != "전체":
        result_df = result_df[result_df["브랜드"] == selected_brand]
    
    tab1, tab2, tab3 = st.tabs(["📊 전체배정및집행결과", "👥 인플루언서별", "📋 실제집행수관리"])
    with tab1:
        st.subheader("📊 전체배정및집행결과")
        
        # 전체 배정 결과 표시 (필터링 없이)
        if 'result_df' in st.session_state:
            all_results = st.session_state['result_df']
        else:
            # 배정 이력에서 전체 결과 로드
            if os.path.exists(assignment_history_file):
                all_results = pd.read_csv(assignment_history_file, encoding="utf-8")
                if not all_results.empty:
                    # 실제집행수 컬럼 추가
                    if os.path.exists(execution_status_file):
                        execution_data = pd.read_csv(execution_status_file, encoding="utf-8")
                        if not execution_data.empty:
                            # 전체 실제집행수 계산
                            total_execution = execution_data.groupby("ID")["실제집행수"].sum().reset_index()
                            total_execution.columns = ["ID", "전체_실집행수"]
                            
                            # 브랜드별 실제집행수 계산
                            brand_execution = execution_data.groupby(["ID", "브랜드"])["실제집행수"].sum().reset_index()
                            brand_execution.columns = ["ID", "브랜드", "브랜드_실집행수"]
                            
                            # all_results에 실제집행수 컬럼 추가
                            all_results = all_results.merge(total_execution, on="ID", how="left")
                            all_results = all_results.merge(brand_execution, on=["ID", "브랜드"], how="left")
                            
                            # NaN 값을 0으로 채우기
                            if "전체_실집행수" in all_results.columns:
                                all_results["전체_실집행수"] = all_results["전체_실집행수"].fillna(0)
                            else:
                                all_results["전체_실집행수"] = 0
                                
                            if "브랜드_실집행수" in all_results.columns:
                                all_results["브랜드_실집행수"] = all_results["브랜드_실집행수"].fillna(0)
                            else:
                                all_results["브랜드_실집행수"] = 0
                        else:
                            all_results["전체_실집행수"] = 0
                            all_results["브랜드_실집행수"] = 0
                    else:
                        all_results["전체_실집행수"] = 0
                        all_results["브랜드_실집행수"] = 0
                else:
                    all_results = pd.DataFrame()
            else:
                all_results = pd.DataFrame()
        
        # 브랜드 필터 적용
        if selected_brand != "전체" and not all_results.empty:
            all_results = all_results[all_results["브랜드"] == selected_brand]
        
        if not all_results.empty:
            st.dataframe(all_results, use_container_width=True)
            st.download_button("📥 전체배정및집행결과 엑셀 다운로드", to_excel_bytes(all_results), file_name="all_assigned_result.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="all_assigned_result_xlsx_2")
        else:
            st.info("전체 배정 결과가 없습니다.")
    with tab2:
        st.subheader("👥 인플루언서별 배정 현황")
        if not influencer_summary.empty:
            st.dataframe(influencer_summary, use_container_width=True)
            st.download_button("📥 인플루언서별 배정 현황 엑셀 다운로드", to_excel_bytes(influencer_summary), file_name="influencer_summary.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="influencer_summary_xlsx")
        else:
            st.info("배정 이력이 없습니다.")
    with tab3:
        st.subheader("📋 실제집행수 관리")
        
        # 템플릿 다운로드
        st.subheader("📄 템플릿 다운로드")
        template_df = create_execution_template()
        # 현재 일시를 파일명에 추가
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        template_filename = f"execution_template_{current_time}.xlsx"
        
        st.download_button(
            "📥 실제집행수 템플릿 다운로드", 
            to_excel_bytes(template_df), 
            file_name=template_filename, 
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
            key="template_download_2"
        )
        
        st.info("💡 배정 결과 기반 템플릿을 다운로드하여 실제집행수를 입력한 후 업로드하세요.")
        st.info("💡 실제집행수: 0 (집행하지 않음) 또는 1 (집행함)")
        st.info("💡 이전 입력 결과가 템플릿에 자동으로 포함됩니다.")
        
        # 실제집행수 파일 업로드
        uploaded_file = st.file_uploader("실제집행수 엑셀 파일 업로드", type=['xlsx', 'xls'], key="execution_upload_2")
        
        if uploaded_file is not None:
            try:
                uploaded_df = pd.read_excel(uploaded_file)
                
                # 필수 컬럼 확인
                required_columns = ["브랜드", "ID", "이름", "배정월", "계획수", "실제집행수"]
                missing_columns = [col for col in required_columns if col not in uploaded_df.columns]
                
                if missing_columns:
                    st.error(f"❌ 필수 컬럼이 누락되었습니다: {', '.join(missing_columns)}")
                else:
                    st.success("✅ 파일이 성공적으로 업로드되었습니다.")
                    st.dataframe(uploaded_df, use_container_width=True)
                    
                    # 실제집행수 유효성 검사
                    invalid_execution = uploaded_df[~uploaded_df["실제집행수"].isin([0, 1])]
                    if not invalid_execution.empty:
                        st.error("❌ 실제집행수는 0 또는 1만 입력 가능합니다.")
                    else:
                        # 저장 버튼
                        if st.button("💾 실제집행수 저장", key="save_execution_2"):
                            uploaded_df.to_csv(execution_status_file, index=False, encoding="utf-8")
                            st.success("✅ 실제집행수가 성공적으로 저장되었습니다.")
                            st.rerun()
            except Exception as e:
                st.error(f"❌ 파일 읽기 오류: {str(e)}")
        
        # 현재 실제집행수 상태 표시
        st.subheader("📊 현재 실제집행수 상태")
        if os.path.exists(execution_status_file):
            current_execution = pd.read_csv(execution_status_file, encoding="utf-8")
            if not current_execution.empty:
                st.dataframe(current_execution, use_container_width=True)
                st.download_button("📥 현재 실제집행수 다운로드", to_excel_bytes(current_execution), file_name="current_execution_status.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="current_execution_download_2")
            else:
                st.info("저장된 실제집행수 데이터가 없습니다.")
        else:
            st.info("실제집행수 파일이 없습니다.")