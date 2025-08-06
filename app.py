import streamlit as st
import pandas as pd
import os
from datetime import datetime
import io

# 페이지 설정
st.set_page_config(page_title="인플루언서 배정 앱", layout="wide")

# 유틸리티 함수들
def to_excel_bytes(df):
    """DataFrame을 Excel 바이트로 변환"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)
    return output.getvalue()

def create_execution_template():
    """실제집행수 입력용 템플릿 생성"""
    if os.path.exists("data/assignment_history.csv"):
        assignment_history = pd.read_csv("data/assignment_history.csv", encoding="utf-8")
        
        # 기존 실행 상태 로드
        if os.path.exists("data/execution_status.csv"):
            existing_execution = pd.read_csv("data/execution_status.csv", encoding="utf-8")
        else:
            existing_execution = pd.DataFrame()
        
        # 템플릿 생성
        available_columns = ["브랜드", "ID", "이름", "배정월"]
        if "계획수" in assignment_history.columns:
            available_columns.append("계획수")
        else:
            # 계획수 컬럼이 없으면 기본값 1로 생성
            assignment_history["계획수"] = 1
            available_columns.append("계획수")
        
        template_df = assignment_history[available_columns].copy()
        
        # 기존 실행 상태와 병합
        if not existing_execution.empty:
            template_df = template_df.merge(existing_execution[["ID", "브랜드", "배정월", "실제집행수"]], 
                                          on=["ID", "브랜드", "배정월"], how="left")
        
        # 실제집행수 컬럼이 없으면 생성
        if "실제집행수" not in template_df.columns:
            template_df["실제집행수"] = 0
        else:
            template_df["실제집행수"] = template_df["실제집행수"].fillna(0)
        
        return template_df
    else:
        return pd.DataFrame(columns=["브랜드", "ID", "이름", "배정월", "계획수", "실제집행수"])

def safe_fillna(df, column, default_value=0):
    """안전하게 컬럼에 fillna 적용"""
    if column in df.columns:
        df[column] = df[column].fillna(default_value)
    else:
        df[column] = default_value
    return df

def add_execution_data(df, execution_file="data/execution_status.csv"):
    """실행 데이터를 DataFrame에 추가"""
    if os.path.exists(execution_file):
        execution_data = pd.read_csv(execution_file, encoding="utf-8")
        if not execution_data.empty:
            # 전체 실행수 계산 (ID별로 모든 브랜드의 실제집행수 합계)
            total_execution = execution_data.groupby("ID")["실제집행수"].sum().reset_index()
            total_execution.columns = ["ID", "전체_실집행수"]
            
            # 브랜드별 실행수 계산 (ID와 브랜드별로 실제집행수 합계)
            brand_execution = execution_data.groupby(["ID", "브랜드"])["실제집행수"].sum().reset_index()
            brand_execution.columns = ["ID", "브랜드", "브랜드_실집행수"]
            
            # 실제집행수 계산 완료
            pass
            
            # 기존 컬럼이 있으면 제거
            if "전체_실집행수" in df.columns:
                df = df.drop("전체_실집행수", axis=1)
            if "브랜드_실집행수" in df.columns:
                df = df.drop("브랜드_실집행수", axis=1)
            
            # 병합
            df = df.merge(total_execution, on="ID", how="left")
            df = df.merge(brand_execution, on=["ID", "브랜드"], how="left")
            
            # 안전하게 fillna 적용
            df = safe_fillna(df, "전체_실집행수")
            df = safe_fillna(df, "브랜드_실집행수")
            
            # 실제집행수 계산 완료
            pass
            
        else:
            df["전체_실집행수"] = 0
            df["브랜드_실집행수"] = 0
    else:
        df["전체_실집행수"] = 0
        df["브랜드_실집행수"] = 0
    
    return df

def reorder_columns(df, expected_columns):
    """컬럼 순서 재정렬"""
    available_columns = [col for col in expected_columns if col in df.columns]
    if available_columns:
        return df[available_columns]
    return df

# 메인 앱
def main():
    # 페이지 설정
    st.set_page_config(
        page_title="인플루언서 배정 앱",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # SEO 메타데이터
    st.markdown("""
    <head>
        <meta name="description" content="인플루언서 배정 및 실제집행수 관리 시스템">
        <meta name="keywords" content="인플루언서, 배정, 관리, Streamlit">
        <meta name="author" content="인플루언서 배정 앱">
    </head>
    """, unsafe_allow_html=True)
    
    st.title("🎯 인플루언서 배정 앱")
    
    # 데이터 파일 경로
    influencer_file = "data/influencer.csv"
    assignment_file = "data/assignment_history.csv"
    execution_file = "data/execution_status.csv"
    
    # 인플루언서 데이터 로드
    if os.path.exists(influencer_file):
        df = pd.read_csv(influencer_file, encoding="utf-8")
        df.columns = df.columns.str.strip()
    else:
        st.error("인플루언서 데이터 파일이 없습니다.")
        return
    
    # 사이드바 설정
    st.sidebar.header("📋 배정 설정")
    
    # 계절 선택
    season = st.sidebar.selectbox("계절", ["25FW"])
    
    # 배정월 선택
    month_options = ["9월", "10월", "11월", "12월", "1월", "2월"]
    selected_month = st.sidebar.selectbox("배정월", month_options)
    
    # 브랜드별 배정 수량
    st.sidebar.subheader("브랜드별 배정 수량")
    mlb_qty = st.sidebar.number_input("MLB", min_value=0, value=0)
    dx_qty = st.sidebar.number_input("DX", min_value=0, value=0)
    dv_qty = st.sidebar.number_input("DV", min_value=0, value=0)
    st_qty = st.sidebar.number_input("ST", min_value=0, value=0)
    
    # 배정 실행 버튼
    assignment_blocked = False  # 초기화
    
    if st.sidebar.button("🚀 배정실행") and not assignment_blocked:
        # 실제집행수가 입력된 인플루언서가 있는지 체크
        if os.path.exists(execution_file):
            execution_data = pd.read_csv(execution_file, encoding="utf-8")
            if not execution_data.empty:
                # 선택된 월에 실제집행수가 입력된 인플루언서가 있는지 확인
                existing_executions = execution_data[execution_data["배정월"] == selected_month]
                if not existing_executions.empty:
                    st.error("⚠️ 실집행수가 입력된 인플루언서가 있습니다.")
                    st.write(f"다음 인플루언서들의 {selected_month} 실집행수가 이미 입력되어 있습니다:")
                    
                    # 실제집행수가 입력된 인플루언서 목록 표시
                    for _, row in existing_executions.iterrows():
                        st.write(f"• {row['브랜드']} - {row['이름']} ({row['ID']})")
                    
                    st.write("배정 초기화 후 진행해주세요.")
                    
                    # 확인 버튼
                    if st.button("확인", type="primary"):
                        st.rerun()
                    
                    # 배정 실행 버튼 비활성화
                    st.sidebar.warning("⚠️ 실집행수가 입력된 인플루언서가 있어 배정이 불가능합니다.")
                    assignment_blocked = True
                    return  # 여기서 함수 종료하여 배정 로직 실행 방지
        # 2. 이전 월 실제집행수 완료 여부 검증 (모든 이전 월 체크)
        month_order = ["9월", "10월", "11월", "12월", "1월", "2월"]
        current_month_index = month_order.index(selected_month)
        
        if current_month_index > 0:  # 첫 번째 월이 아닌 경우
            # 모든 이전 월들을 체크
            previous_months = month_order[:current_month_index]
        
        # 모든 이전 월의 배정과 실제집행수 완료 여부 체크
        if current_month_index > 0:  # 첫 번째 월이 아닌 경우
            if os.path.exists(assignment_file):
                existing_history = pd.read_csv(assignment_file, encoding="utf-8")
                
                # 모든 이전 월들을 체크
                for check_month in previous_months:
                    previous_assignments = existing_history[existing_history["배정월"] == check_month]
                    
                    # 해당 월에 배정이 있는 경우 실제집행수 체크
                    if not previous_assignments.empty:
                        # 해당 월의 실제집행수 확인
                        if os.path.exists(execution_file):
                            execution_data = pd.read_csv(execution_file, encoding="utf-8")
                            previous_executions = execution_data[execution_data["배정월"] == check_month]
                            
                            # 해당 월에 배정되었지만 실제집행수가 없는 경우 체크
                            missing_executions = []
                            for _, assignment in previous_assignments.iterrows():
                                execution_exists = previous_executions[
                                    (previous_executions["ID"] == assignment["ID"]) & 
                                    (previous_executions["브랜드"] == assignment["브랜드"])
                                ]
                                if execution_exists.empty:
                                    missing_executions.append(f"{assignment['브랜드']} - {assignment['이름']} ({assignment['ID']})")
                            
                            if missing_executions:
                                # 페이지 내 알림문구 표시
                                st.error(f"⚠️ {check_month} 실제집행수가 업데이트되지 않았습니다!")
                                st.write(f"다음 인플루언서들의 {check_month} 실제집행수를 먼저 입력해주세요:")
                                
                                # 누락된 인플루언서 목록 표시
                                for missing in missing_executions:
                                    st.write(f"• {missing}")
                                
                                st.write("📋 실제집행수관리 탭에서 템플릿을 다운로드하여 실제집행수를 입력한 후 다시 시도해주세요.")
                                
                                # 확인 버튼
                                if st.button("확인", type="primary"):
                                    st.rerun()
                                
                                # 배정 실행 버튼 비활성화
                                st.sidebar.warning(f"⚠️ {check_month} 실제집행수 업데이트 후 배정이 가능합니다.")
                                assignment_blocked = True
                                return  # 여기서 함수 종료하여 배정 로직 실행 방지
                        else:
                            # 해당 월에 배정은 있지만 실제집행수 파일이 없는 경우
                            st.error(f"⚠️ {check_month} 배정이 있지만 실제집행수가 입력되지 않았습니다!")
                            st.write(f"다음 인플루언서들의 {check_month} 실제집행수를 먼저 입력해주세요:")
                            
                            for _, assignment in previous_assignments.iterrows():
                                st.write(f"• {assignment['브랜드']} - {assignment['이름']} ({assignment['ID']})")
                            
                            st.write("📋 실제집행수관리 탭에서 템플릿을 다운로드하여 실제집행수를 입력한 후 다시 시도해주세요.")
                            
                            if st.button("확인", type="primary"):
                                st.rerun()
                            
                            st.sidebar.warning(f"⚠️ {check_month} 실제집행수 업데이트 후 배정이 가능합니다.")
                            assignment_blocked = True
                            return
                
                # 모든 이전 월이 완료된 경우 - 배정 가능
                assignment_blocked = False
            else:
                # 배정 이력 파일이 없는 경우 - 배정 가능
                assignment_blocked = False
        else:
            # 첫 번째 월인 경우 - 배정 가능
            assignment_blocked = False
        
        # 3. 데이터 무결성 검증
        if os.path.exists(assignment_file):
            existing_history = pd.read_csv(assignment_file, encoding="utf-8")
            
            # 배정 이력과 실제집행수 데이터 간의 일관성 검증
            if os.path.exists(execution_file) and not existing_history.empty:
                execution_data = pd.read_csv(execution_file, encoding="utf-8")
                if not execution_data.empty:
                    # 배정은 있지만 실제집행수가 없는 경우 검증
                    for _, assignment in existing_history.iterrows():
                        execution_exists = execution_data[
                            (execution_data["ID"] == assignment["ID"]) & 
                            (execution_data["브랜드"] == assignment["브랜드"]) &
                            (execution_data["배정월"] == assignment["배정월"])
                        ]
                        if execution_exists.empty:
                            st.warning(f"⚠️ 데이터 불일치: {assignment['브랜드']} - {assignment['이름']} ({assignment['ID']})의 {assignment['배정월']} 배정에 대한 실제집행수가 없습니다.")
        else:
            existing_history = pd.DataFrame()
        
        # 실행 데이터 로드하여 잔여 수량 계산에 반영
        df_copy = df.copy()
        
        # 기존 배정 횟수를 잔여 수량에서 차감
        if not existing_history.empty:
            for _, row in existing_history.iterrows():
                influencer_id = row["ID"]
                brand = row["브랜드"]
                if influencer_id in df_copy["id"].values:
                    brand_qty_col = f"{brand.lower()}_qty"
                    if brand_qty_col in df_copy.columns:
                        current_qty = df_copy.loc[df_copy["id"] == influencer_id, brand_qty_col].iloc[0]
                        df_copy.loc[df_copy["id"] == influencer_id, brand_qty_col] = max(0, current_qty - 1)
        
        # 실제집행수 차감
        if os.path.exists(execution_file):
            execution_data = pd.read_csv(execution_file, encoding="utf-8")
            if not execution_data.empty:
                # 브랜드별 실행수 계산
                for brand in ["MLB", "DX", "DV", "ST"]:
                    brand_execution = execution_data[execution_data["브랜드"] == brand].groupby("ID")["실제집행수"].sum()
                    qty_col = f"{brand.lower()}_qty"
                    if qty_col in df_copy.columns:
                        df_copy[qty_col] = df_copy[qty_col] - df_copy["id"].map(brand_execution).fillna(0)
                        df_copy[qty_col] = df_copy[qty_col].clip(lower=0)
        
        # 배정 로직
        results = []
        brands = [("MLB", mlb_qty), ("DX", dx_qty), ("DV", dv_qty), ("ST", st_qty)]
        
        for brand, qty in brands:
            if qty > 0:
                brand_df = df_copy[df_copy[f"{brand.lower()}_qty"] > 0].copy()
                brand_df = brand_df.sort_values("follower", ascending=False)
                
                for i, row in brand_df.head(qty).iterrows():
                    original_brand_qty = row[f"{brand.lower()}_qty"]
                    original_total_qty = sum(row[f"{b.lower()}_qty"] for b in ["MLB", "DX", "DV", "ST"])
                    
                    # 잔여 수량 계산
                    brand_remaining = max(0, original_brand_qty - 1)
                    total_remaining = max(0, original_total_qty - 1)
                    
                    # 브랜드별 실행수 계산
                    brand_execution = 0
                    total_execution = 0
                    if os.path.exists(execution_file):
                        execution_data = pd.read_csv(execution_file, encoding="utf-8")
                        if not execution_data.empty:
                            brand_execution = execution_data[(execution_data["ID"] == row["id"]) & 
                                                          (execution_data["브랜드"] == brand)]["실제집행수"].sum()
                            total_execution = execution_data[execution_data["ID"] == row["id"]]["실제집행수"].sum()
                    
                    results.append({
                        "브랜드": brand,
                        "ID": row["id"],
                        "이름": row["name"],
                        "배정월": selected_month,
                        "FLW": row["follower"],
                        "브랜드_계약수": original_brand_qty,
                        "브랜드_실집행수": brand_execution,
                        "브랜드_잔여수": brand_remaining,
                        "전체_계약수": original_total_qty,
                        "전체_실집행수": total_execution,
                        "전체_잔여수": total_remaining
                    })
                    
                    # 잔여 수량 업데이트
                    df_copy.loc[i, f"{brand.lower()}_qty"] = brand_remaining
        
        # 결과 저장
        if results:
            result_df = pd.DataFrame(results)
            
            # 기존 배정 이력 로드
            if os.path.exists(assignment_file):
                existing_history = pd.read_csv(assignment_file, encoding="utf-8")
                updated_history = pd.concat([existing_history, result_df], ignore_index=True)
            else:
                updated_history = result_df
            
            # 배정 이력 저장
            updated_history.to_csv(assignment_file, index=False, encoding="utf-8")
            
            st.session_state['result_df'] = result_df
            st.session_state['selected_month'] = selected_month
            st.success(f"✅ {selected_month} 배정이 완료되었습니다!")
            st.rerun()
    
    # 탭 생성
    tab1, tab2, tab3 = st.tabs(["📊 전체배정및집행결과", "👥 인플루언서별", "📋 실제집행수관리"])
    
    with tab1:
        st.subheader("📊 전체배정및집행결과")
        
        # 월별 필터
        selected_month_filter = st.selectbox("📅 월별 필터", month_options, index=0)
        
        # 브랜드 필터
        selected_brand_filter = st.selectbox("🏷️ 브랜드 필터", ["전체", "MLB", "DX", "DV", "ST"], index=0)
        
        # 배정 결과 로드 및 표시
        if os.path.exists(assignment_file):
            assignment_history = pd.read_csv(assignment_file, encoding="utf-8")
            
            if not assignment_history.empty:
                # 실행 데이터 추가
                all_results = add_execution_data(assignment_history, execution_file)
                
                # 필터 적용
                all_results = all_results[all_results["배정월"] == selected_month_filter]
                if selected_brand_filter != "전체":
                    all_results = all_results[all_results["브랜드"] == selected_brand_filter]
                
                # 컬럼 순서 정리
                expected_columns = ["브랜드", "ID", "이름", "배정월", "FLW", "브랜드_계약수", 
                                  "브랜드_실집행수", "브랜드_잔여수", "전체_계약수", "전체_실집행수", "전체_잔여수"]
                all_results = reorder_columns(all_results, expected_columns)
                
                if not all_results.empty:
                    st.dataframe(all_results, use_container_width=True)
                    
                    # 다운로드 버튼
                    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"assignment_results_{current_time}.xlsx"
                    st.download_button(
                        "📥 전체배정및집행결과 엑셀 다운로드",
                        to_excel_bytes(all_results),
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
                    # 배정 초기화 버튼
                    col1, col2 = st.columns([1, 4])
                    with col1:
                        if st.button("🗑️ 배정초기화", type="secondary"):
                            if selected_month_filter == "전체":
                                # 전체 초기화
                                os.remove(assignment_file)
                                # 실제집행수 파일도 삭제
                                if os.path.exists(execution_file):
                                    os.remove(execution_file)
                            else:
                                # 해당 월에 배정된 인플루언서 ID 목록 먼저 가져오기
                                assigned_influencers = assignment_history[assignment_history["배정월"] == selected_month_filter]["ID"].unique()
                                
                                # 월 순서 정의
                                month_order = ["9월", "10월", "11월", "12월", "1월", "2월"]
                                selected_month_index = month_order.index(selected_month_filter)
                                
                                # 선택된 월과 이후 월들의 배정 모두 삭제 (잔여 계산 무효화 방지)
                                months_to_remove = month_order[selected_month_index:]
                                assignment_history = assignment_history[~assignment_history["배정월"].isin(months_to_remove)]
                                assignment_history.to_csv(assignment_file, index=False, encoding="utf-8")
                                
                                # 선택된 월과 이후 월들의 실제집행수 삭제
                                if os.path.exists(execution_file):
                                    execution_data = pd.read_csv(execution_file, encoding="utf-8")
                                    
                                    # 선택된 월과 이후 월들의 실제집행수 삭제
                                    execution_data = execution_data[~execution_data["배정월"].isin(months_to_remove)]
                                    
                                    if not execution_data.empty:
                                        execution_data.to_csv(execution_file, index=False, encoding="utf-8")
                                    else:
                                        # 모든 데이터가 삭제되면 파일 삭제
                                        os.remove(execution_file)
                            
                            st.success("✅ 초기화가 완료되었습니다!")
                            st.rerun()
                else:
                    st.info("해당 조건의 배정 결과가 없습니다.")
            else:
                st.info("배정 이력이 없습니다.")
        else:
            st.info("배정 이력이 없습니다.")
    
    with tab2:
        st.subheader("👥 인플루언서별 배정 현황")
        
        # 인플루언서 요약 생성
        influencer_summary = df[["id", "name", "follower", "unit_fee"]].copy()
        brand_list = ["MLB", "DX", "DV", "ST"]
        qty_cols = [f"{brand.lower()}_qty" for brand in brand_list]
        influencer_summary["전체계약횟수"] = df[qty_cols].sum(axis=1)
        
        # 배정 이력에서 집행 횟수 계산
        if os.path.exists(assignment_file):
            assignment_history = pd.read_csv(assignment_file, encoding="utf-8")
            exec_counts = assignment_history.groupby("ID").size()
            influencer_summary["전체집행횟수"] = influencer_summary["id"].map(exec_counts).fillna(0).astype(int)
        else:
            influencer_summary["전체집행횟수"] = 0
        
        # 브랜드별 잔여 횟수 계산
        for brand in brand_list:
            qty_col = f"{brand.lower()}_qty"
            if qty_col in df.columns:
                max_qty = df[qty_col]
            else:
                max_qty = 0
            
            if os.path.exists(assignment_file):
                brand_counts = assignment_history[assignment_history["브랜드"] == brand].groupby("ID").size()
                influencer_summary[f"잔여횟수_{brand}"] = influencer_summary.apply(
                    lambda row: max(0, max_qty.get(row.name, 0) - brand_counts.get(row["id"], 0)), axis=1
                )
            else:
                influencer_summary[f"잔여횟수_{brand}"] = influencer_summary.apply(
                    lambda row: max_qty.get(row.name, 0), axis=1
                )
        
        # 월별 컬럼 추가
        months = ["9월", "10월", "11월", "12월", "1월", "2월"]
        for month in months:
            influencer_summary[month] = ""
        
        # 배정 이력에서 월별 브랜드 정보 채우기
        if os.path.exists(assignment_file):
            for _, row in assignment_history.iterrows():
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
            "id": "ID", "name": "이름", "follower": "FLW", "unit_fee": "1회계약단가"
        })
        
        if not influencer_summary.empty:
            st.dataframe(influencer_summary, use_container_width=True)
            st.download_button(
                "📥 인플루언서별 배정 현황 엑셀 다운로드",
                to_excel_bytes(influencer_summary),
                file_name="influencer_summary.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("인플루언서 데이터가 없습니다.")
    
    with tab3:
        st.subheader("📋 실제집행수 관리")
        
        # 템플릿 다운로드
        st.subheader("📄 템플릿 다운로드")
        template_df = create_execution_template()
        
        if not template_df.empty:
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            template_filename = f"execution_template_{current_time}.xlsx"
            
            st.download_button(
                "📥 실제집행수 템플릿 다운로드",
                to_excel_bytes(template_df),
                file_name=template_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
            st.info("💡 배정 결과 기반 템플릿을 다운로드하여 실제집행수를 입력한 후 업로드하세요.")
            st.info("💡 실제집행수: 0 (집행하지 않음) 또는 1 (집행함)")
            st.info("💡 이전 입력 결과가 템플릿에 자동으로 포함됩니다.")
        else:
            st.info("배정 이력이 없어 템플릿을 생성할 수 없습니다.")
        
        # 실제집행수 파일 업로드
        uploaded_file = st.file_uploader("실제집행수 엑셀 파일 업로드", type=['xlsx', 'xls'])
        
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
                        if st.button("💾 실제집행수 저장"):
                            # 5. 저장 전 최종 검증
                            validation_errors = []
                            
                            # 배정 이력과 일치하는지 확인
                            if os.path.exists(assignment_file):
                                assignment_history = pd.read_csv(assignment_file, encoding="utf-8")
                                for _, row in uploaded_df.iterrows():
                                    assignment_exists = assignment_history[
                                        (assignment_history["ID"] == row["ID"]) & 
                                        (assignment_history["브랜드"] == row["브랜드"]) &
                                        (assignment_history["배정월"] == row["배정월"])
                                    ]
                                    if assignment_exists.empty:
                                        validation_errors.append(f"배정 이력에 없는 데이터: {row['브랜드']} - {row['이름']} ({row['ID']})의 {row['배정월']} 배정")
                            
                            # 중복 데이터 확인
                            if os.path.exists(execution_file):
                                existing_execution = pd.read_csv(execution_file, encoding="utf-8")
                                for _, row in uploaded_df.iterrows():
                                    duplicate_exists = existing_execution[
                                        (existing_execution["ID"] == row["ID"]) & 
                                        (existing_execution["브랜드"] == row["브랜드"]) &
                                        (existing_execution["배정월"] == row["배정월"])
                                    ]
                                    if not duplicate_exists.empty:
                                        validation_errors.append(f"중복 데이터: {row['브랜드']} - {row['이름']} ({row['ID']})의 {row['배정월']} 실제집행수")
                            
                            if validation_errors:
                                st.error("❌ 저장 전 검증 실패:")
                                for error in validation_errors:
                                    st.write(f"• {error}")
                                st.write("💡 배정 초기화 후 다시 시도하거나, 중복 데이터를 제거해주세요.")
                            else:
                                # 기존 데이터와 병합하여 저장
                                if os.path.exists(execution_file):
                                    existing_execution = pd.read_csv(execution_file, encoding="utf-8")
                                    updated_execution = pd.concat([existing_execution, uploaded_df], ignore_index=True)
                                else:
                                    updated_execution = uploaded_df
                                
                                updated_execution.to_csv(execution_file, index=False, encoding="utf-8")
                                st.success("✅ 실제집행수가 성공적으로 저장되었습니다!")
                                st.rerun()
            except Exception as e:
                st.error(f"❌ 파일 읽기 오류: {str(e)}")
        
        # 현재 실제집행수 상태 표시
        st.subheader("📊 현재 실제집행수 상태")
        if os.path.exists(execution_file):
            current_execution = pd.read_csv(execution_file, encoding="utf-8")
            if not current_execution.empty:
                st.dataframe(current_execution, use_container_width=True)
                st.download_button(
                    "📥 현재 실제집행수 다운로드",
                    to_excel_bytes(current_execution),
                    file_name="current_execution_status.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.info("저장된 실제집행수 데이터가 없습니다.")
        else:
            st.info("실제집행수 파일이 없습니다.")

if __name__ == "__main__":
    main()