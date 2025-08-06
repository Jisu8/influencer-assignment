# 인플루언서 배정 앱

인플루언서 배정 및 실제집행수 관리를 위한 Streamlit 애플리케이션입니다.

## 주요 기능

- **인플루언서 배정**: 브랜드별, 월별 인플루언서 배정
- **실제집행수 관리**: 배정 결과에 대한 실제집행수 입력 및 관리
- **데이터 무결성 검증**: 순차적 배정 및 실제집행수 완료 검증
- **엑셀 다운로드**: 배정 결과 및 실제집행수 템플릿 다운로드

## 설치 및 실행

### 로컬 실행
```bash
pip install -r requirements.txt
streamlit run app.py
```

### 배포 방법

#### 1. Streamlit Cloud (추천)
1. [Streamlit Cloud](https://streamlit.io/cloud)에 가입
2. GitHub에 코드 업로드
3. Streamlit Cloud에서 GitHub 저장소 연결
4. 자동 배포 완료

#### 2. Heroku
1. Heroku 계정 생성
2. Heroku CLI 설치
3. `heroku create your-app-name`
4. `git push heroku main`

#### 3. Railway
1. [Railway](https://railway.app/)에 가입
2. GitHub 저장소 연결
3. 자동 배포

## 파일 구조

```
influencer/
├── app.py                 # 메인 애플리케이션
├── requirements.txt       # Python 의존성
├── README.md             # 프로젝트 설명
└── data/                 # 데이터 파일들
    ├── influencer.csv     # 인플루언서 기본 데이터
    ├── assignment_history.csv  # 배정 이력
    └── execution_status.csv    # 실제집행수 데이터
```

## 사용법

1. **배정 설정**: 사이드바에서 계절, 배정월, 브랜드별 수량 설정
2. **배정 실행**: "배정실행" 버튼으로 인플루언서 배정
3. **실제집행수 관리**: "실제집행수관리" 탭에서 템플릿 다운로드 및 업로드
4. **결과 확인**: "전체배정및집행결과" 탭에서 배정 결과 확인

## 데이터 무결성

- 이전 월의 실제집행수가 완료되지 않으면 이후 월 배정 차단
- 배정된 모든 월의 실제집행수 입력 필수
- 순차적 배정 보장 