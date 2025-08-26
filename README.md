# 인플루언서 배정 관리 시스템

인플루언서 배정 및 집행 관리를 위한 Streamlit 애플리케이션입니다.

## 주요 기능

### 📊 배정 및 집행결과 탭
- **배정 현황 조회**: 월별, 브랜드별 배정 결과 확인
- **집행 상태 관리**: 배정완료 → 집행완료 상태 변경
- **엑셀 업로드**: 배정 상태 및 집행 URL 업데이트
- **자동 정보 채우기**: ID만 입력하면 이름, 팔로워, 계약단가 등 자동 입력

### 👥 인플루언서별 탭
- **브랜드별 현황**: 브랜드 필터링으로 인플루언서별 배정 현황 확인
- **월별 집행 현황**: 각 월별 집행 완료된 브랜드 표시
- **계약 정보**: 1회계약단가, 2차활용 등 상세 정보 표시

### 🚀 자동/수동 배정
- **자동 배정**: 브랜드별 수량 설정으로 자동 배정
- **수동 배정**: 개별 인플루언서 수동 배정
- **잔여수 확인**: 계약수 대비 잔여수 기반 배정 제한

## 설치 및 실행

### 로컬 실행
```bash
pip install -r requirements.txt
streamlit run fnfcrew.py
```

### Streamlit Cloud 배포

1. **GitHub 저장소 준비**
   - 현재 저장소: `https://github.com/Jisu8/influencer-assignment.git`

2. **Streamlit Cloud 배포**
   - [Streamlit Cloud](https://streamlit.io/cloud)에 가입
   - "New app" 클릭
   - GitHub 저장소 연결: `Jisu8/influencer-assignment`
   - Main file path: `fnfcrew.py`
   - "Deploy" 클릭

3. **배포 완료**
   - 자동으로 공개 URL 생성
   - GitHub 코드 변경 시 자동 재배포

## 파일 구조

```
influencer-assignment/
├── fnfcrew.py                    # 메인 애플리케이션
├── requirements.txt              # Python 의존성
├── README.md                     # 프로젝트 설명
├── data/                         # 데이터 파일들
│   ├── influencer.csv            # 인플루언서 기본 데이터
│   ├── assignment_history.csv    # 배정 이력
│   ├── execution_status.csv      # 집행 데이터
│   └── fnfcrew.xlsx             # 원본 데이터
└── backup files/                 # 백업 파일들
```

## 사용법

### 1. 배정 관리
- **자동 배정**: 사이드바에서 브랜드별 수량 설정 후 "자동 배정실행"
- **수동 배정**: 인플루언서 ID 입력 후 "수동 배정저장"

### 2. 집행 관리
- **집행 상태 변경**: 배정결과 탭에서 "결과" 컬럼 수정
- **집행 URL 입력**: 집행 완료 시 URL 입력
- **엑셀 업로드**: 대량 업데이트 시 엑셀 파일 업로드

### 3. 현황 조회
- **배정 및 집행결과**: 전체 배정 현황 및 집행 상태 확인
- **인플루언서별**: 브랜드별, 시즌별 인플루언서 현황 조회

## 데이터 무결성

- **순차적 배정**: 이전 월 집행 완료 후 다음 월 배정 가능
- **잔여수 제한**: 계약수 대비 잔여수 기반 배정 제한
- **자동 정보 연동**: influencer.csv 기반 기본 정보 자동 채우기

## 기술 스택

- **Frontend**: Streamlit
- **Backend**: Python, Pandas
- **Data**: CSV, Excel
- **Deployment**: Streamlit Cloud 