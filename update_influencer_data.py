#!/usr/bin/env python3
"""
FNF Crew 데이터 배치 스크립트
fnfcrew.xlsx → influencer.csv 변환
"""

import pandas as pd
import os
from datetime import datetime

def process_fnfcrew_data(df):
    """FNF Crew 데이터 처리"""
    # 컬럼명 매핑
    column_mapping = {
        'sns_id': 'id',
        'mlb_cnt': 'mlb_qty',
        'dx_cnt': 'dx_qty',
        'dv_cnt': 'dv_qty',
        'st_cnt': 'st_qty',
        'total_cnt': 'total_qty'
    }
    
    # 컬럼명 변경
    df = df.rename(columns=column_mapping)
    
    # 필수 컬럼 확인
    required_columns = ['id', 'name', 'follower', 'mlb_qty', 'dx_qty', 'dv_qty', 'st_qty', 
                       'total_amt_incl2nd', 'total_amt_exc2nd', 'total_qty', 'contract_sesn', 'sec_usage']
    
    # 누락된 컬럼 확인
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"❌ 필수 컬럼이 누락되었습니다: {missing_columns}")
        return None
    
    # 1회 계약단가 계산
    df['unit_fee'] = 0
    mask = df['total_qty'] > 0
    df.loc[mask, 'unit_fee'] = (
        (df.loc[mask, 'total_amt_incl2nd'].fillna(0) + 
         df.loc[mask, 'total_amt_exc2nd'].fillna(0)) / 
        df.loc[mask, 'total_qty']
    ).astype(int)
    
    return df

def update_influencer_data():
    """influencer.csv 업데이트"""
    original_file = "data/fnfcrew.xlsx"
    processed_file = "data/influencer.csv"
    
    print(f"🔄 데이터 배치 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 원본 파일 확인
    if not os.path.exists(original_file):
        print(f"❌ 원본 파일이 없습니다: {original_file}")
        return False
    
    try:
        # 원본 데이터 로드
        print("📖 원본 데이터 로드 중...")
        df = pd.read_excel(original_file)
        print(f"✅ 원본 데이터 로드 완료: {len(df)}개 행")
        
        # 데이터 처리
        print("⚙️ 데이터 처리 중...")
        processed_df = process_fnfcrew_data(df)
        
        if processed_df is None:
            print("❌ 데이터 처리 실패")
            return False
        
        # CSV로 저장
        print("💾 CSV 파일 저장 중...")
        processed_df.to_csv(processed_file, index=False, encoding="utf-8")
        
        print(f"✅ influencer.csv 업데이트 완료!")
        print(f"📊 처리된 데이터: {len(processed_df)}개 인플루언서")
        print(f"📁 저장 위치: {processed_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        return False

if __name__ == "__main__":
    success = update_influencer_data()
    if success:
        print("\n🎉 데이터 배치가 성공적으로 완료되었습니다!")
    else:
        print("\n💥 데이터 배치 중 오류가 발생했습니다.")
