import pandas as pd
import os

# 파일 경로
ASSIGNMENT_FILE = "data/assignment_history.csv"
EXECUTION_FILE = "data/execution_status.csv"

def reset_september_assignments():
    """9월 배정 데이터 완전 초기화"""
    print("🔄 9월 배정 데이터 초기화 시작...")
    
    # 1. 배정 이력에서 9월 데이터 제거
    if os.path.exists(ASSIGNMENT_FILE):
        assignment_history = pd.read_csv(ASSIGNMENT_FILE, encoding="utf-8")
        print(f"📊 기존 배정 이력: {len(assignment_history)}개")
        
        # 9월 배정 데이터 필터링
        september_mask = assignment_history['배정월'] == '25년 9월'
        september_count = september_mask.sum()
        print(f"🗓️ 9월 배정 데이터: {september_count}개")
        
        if september_count > 0:
            # 9월 데이터 제거
            assignment_history = assignment_history[~september_mask]
            assignment_history.to_csv(ASSIGNMENT_FILE, index=False, encoding="utf-8")
            print(f"✅ 9월 배정 데이터 {september_count}개 제거 완료")
        else:
            print("ℹ️ 9월 배정 데이터가 없습니다")
        
        print(f"📊 남은 배정 이력: {len(assignment_history)}개")
    else:
        print("ℹ️ 배정 이력 파일이 없습니다")
    
    # 2. 집행 상태에서 9월 데이터 제거
    if os.path.exists(EXECUTION_FILE):
        execution_data = pd.read_csv(EXECUTION_FILE, encoding="utf-8")
        print(f"📊 기존 집행 데이터: {len(execution_data)}개")
        
        if not execution_data.empty:
            # 9월 집행 데이터 필터링
            september_mask = execution_data['배정월'] == '25년 9월'
            september_count = september_mask.sum()
            print(f"🗓️ 9월 집행 데이터: {september_count}개")
            
            if september_count > 0:
                # 9월 데이터 제거
                execution_data = execution_data[~september_mask]
                execution_data.to_csv(EXECUTION_FILE, index=False, encoding="utf-8")
                print(f"✅ 9월 집행 데이터 {september_count}개 제거 완료")
            else:
                print("ℹ️ 9월 집행 데이터가 없습니다")
            
            print(f"📊 남은 집행 데이터: {len(execution_data)}개")
        else:
            print("ℹ️ 집행 데이터가 비어있습니다")
    else:
        print("ℹ️ 집행 상태 파일이 없습니다")
    
    print("🎉 9월 배정 데이터 초기화 완료!")

if __name__ == "__main__":
    reset_september_assignments()

