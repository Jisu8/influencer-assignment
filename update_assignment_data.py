import pandas as pd
import os

# 파일 경로
INFLUENCER_FILE = "data/influencer.csv"
ASSIGNMENT_FILE = "data/assignment_history.csv"

def update_assignment_data():
    """기존 배정 데이터의 FLW, 이름, 계약수 정보 업데이트"""
    
    # 인플루언서 데이터 로드
    if not os.path.exists(INFLUENCER_FILE):
        print("❌ influencer.csv 파일이 없습니다.")
        return
    
    influencer_df = pd.read_csv(INFLUENCER_FILE, encoding="utf-8")
    influencer_df.columns = influencer_df.columns.str.strip()
    
    # 배정 데이터 로드
    if not os.path.exists(ASSIGNMENT_FILE):
        print("❌ assignment_history.csv 파일이 없습니다.")
        return
    
    assignment_df = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
    
    # 업데이트된 행 수 카운트
    updated_count = 0
    
    for idx, row in assignment_df.iterrows():
        # ID로 인플루언서 정보 찾기
        influencer_info = influencer_df[influencer_df['id'] == row['ID']]
        
        if not influencer_info.empty:
            influencer_row = influencer_info.iloc[0]
            
            # FLW 업데이트
            if pd.isna(row['FLW']) or row['FLW'] == "" or row['FLW'] == 0:
                assignment_df.loc[idx, 'FLW'] = influencer_row['follower']
                updated_count += 1
            
            # 이름 업데이트
            if pd.isna(row['이름']) or row['이름'] == "":
                assignment_df.loc[idx, '이름'] = influencer_row['name']
                updated_count += 1
            
            # 브랜드별 계약수 업데이트
            if pd.isna(row['브랜드_계약수']) or row['브랜드_계약수'] == "" or row['브랜드_계약수'] == 0:
                brand_qty_col = f"{row['브랜드'].lower()}_qty"
                if brand_qty_col in influencer_df.columns:
                    assignment_df.loc[idx, '브랜드_계약수'] = influencer_row[brand_qty_col]
                    updated_count += 1
    
    # 업데이트된 데이터 저장
    assignment_df.to_csv(ASSIGNMENT_FILE, index=False, encoding="utf-8")
    
    print(f"✅ {updated_count}개의 데이터가 업데이트되었습니다.")
    print(f"📊 총 {len(assignment_df)}개의 배정 데이터 중 {updated_count}개 업데이트")

if __name__ == "__main__":
    update_assignment_data()
