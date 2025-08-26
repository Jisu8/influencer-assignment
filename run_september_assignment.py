import pandas as pd
import os

# 파일 경로
INFLUENCER_FILE = "data/fnfcrew.xlsx"
ASSIGNMENT_FILE = "data/assignment_history.csv"
EXECUTION_FILE = "data/execution_status.csv"

# 브랜드 설정
BRANDS = ["MLB", "DX", "DV", "ST"]

def get_user_requested_quantities():
    """사용자가 요청한 월별 배정 수량"""
    return {
        "25년 9월": {
            "MLB": 60,
            "DX": 80, 
            "DV": 70,
            "ST": 90
        }
    }

def load_influencer_data():
    """인플루언서 데이터 로드"""
    if os.path.exists(INFLUENCER_FILE):
        try:
            df = pd.read_excel(INFLUENCER_FILE, sheet_name='fnfcrew')
            
            # 컬럼명 매핑
            column_mapping = {
                'sns_id': 'id',
                'mlb_cnt': 'mlb_qty',
                'dx_cnt': 'dx_qty',
                'dv_cnt': 'dv_qty',
                'st_cnt': 'st_qty',
                'total_cnt': 'total_qty'
            }
            df = df.rename(columns=column_mapping)
            
            # 필수 컬럼 확인
            required_columns = ['id', 'name', 'follower', 'mlb_qty', 'dx_qty', 'dv_qty', 'st_qty']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"❌ 필수 컬럼이 누락되었습니다: {missing_columns}")
                return None
            
            return df
        except Exception as e:
            print(f"데이터 로드 중 오류가 발생했습니다: {str(e)}")
            return None
    else:
        print("인플루언서 데이터 파일이 없습니다.")
        return None

def load_assignment_history():
    """배정 이력 로드"""
    if os.path.exists(ASSIGNMENT_FILE):
        return pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
    return pd.DataFrame()

def create_assignment_info(influencer, brand, month):
    """배정 정보 생성"""
    return {
        "브랜드": brand,
        "ID": influencer["id"],
        "이름": influencer["name"],
        "배정월": month,
        "FLW": influencer["follower"],
        "브랜드_계약수": influencer[f"{brand.lower()}_qty"],
        "브랜드_실집행수": 0,
        "브랜드_잔여수": influencer[f"{brand.lower()}_qty"],
        "전체_계약수": influencer["total_qty"],
        "전체_실집행수": 0,
        "전체_잔여수": influencer["total_qty"],
        "집행URL": ""
    }

def run_september_assignment():
    """9월 자동배정 실행"""
    print("🔄 9월 자동배정 시작...")
    
    # 데이터 로드
    df = load_influencer_data()
    if df is None:
        return
    
    existing_history = load_assignment_history()
    print(f"📊 기존 배정 이력: {len(existing_history)}개")
    
    # 요청된 수량 가져오기
    requested_quantities = get_user_requested_quantities()
    month = "25년 9월"
    month_quantities = requested_quantities[month]
    
    print(f"📅 {month} 요청 수량:")
    for brand, qty in month_quantities.items():
        print(f"  • {brand}: {qty}개")
    
    # 각 브랜드별 배정
    total_assignments = 0
    new_assignments = []
    
    for brand in BRANDS:
        target_qty = month_quantities[brand]
        print(f"\n🏷️ {brand} 배정 시작 (목표: {target_qty}개)")
        
        # 해당 브랜드 계약수가 있는 인플루언서 필터링
        brand_qty_col = f"{brand.lower()}_qty"
        available_influencers = df[df[brand_qty_col] > 0].copy()
        
        # 팔로워 수로 정렬 (높은 순)
        available_influencers = available_influencers.sort_values('follower', ascending=False)
        
        print(f"  📊 {brand} 계약 가능 인플루언서: {len(available_influencers)}명")
        
        assigned_count = 0
        
        for _, influencer in available_influencers.iterrows():
            if assigned_count >= target_qty:
                break
                
            influencer_id = influencer["id"]
            
            # 이미 해당 월에 배정되었는지 확인
            if not existing_history.empty:
                existing_mask = (
                    (existing_history["ID"] == influencer_id) & 
                    (existing_history["배정월"] == month)
                )
                if existing_mask.any():
                    print(f"    ⚠️ {influencer['name']}({influencer_id}) - 이미 {month} 배정됨")
                    continue
            
            # 해당 브랜드 계약수 확인
            brand_contract_qty = influencer[brand_qty_col]
            
            # 해당 브랜드에 대한 기존 배정 횟수 확인
            if not existing_history.empty:
                existing_brand_assignments = existing_history[
                    (existing_history["ID"] == influencer_id) & 
                    (existing_history["브랜드"] == brand)
                ]
                existing_assignment_count = len(existing_brand_assignments)
            else:
                existing_assignment_count = 0
            
            # 계약수 초과 여부 확인
            if existing_assignment_count >= brand_contract_qty:
                print(f"    ❌ {influencer['name']}({influencer_id}) - {brand} 계약수 초과")
                continue
            
            # 새로운 배정 추가
            new_assignment = create_assignment_info(influencer, brand, month)
            new_assignments.append(new_assignment)
            assigned_count += 1
            total_assignments += 1
            
            print(f"    ✅ {influencer['name']}({influencer_id}) - {brand} 배정")
        
        print(f"  📊 {brand} 배정 완료: {assigned_count}개")
    
    # 배정 결과 저장
    if new_assignments:
        new_df = pd.DataFrame(new_assignments)
        if not existing_history.empty:
            # 기존 데이터와 병합
            combined_df = pd.concat([existing_history, new_df], ignore_index=True)
        else:
            combined_df = new_df
        
        combined_df.to_csv(ASSIGNMENT_FILE, index=False, encoding="utf-8")
        print(f"\n🎉 배정 완료! 총 {total_assignments}개 배정됨")
        print(f"📊 전체 배정 이력: {len(combined_df)}개")
    else:
        print("\n⚠️ 새로운 배정이 없습니다.")

if __name__ == "__main__":
    run_september_assignment()

