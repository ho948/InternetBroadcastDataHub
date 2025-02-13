# 💻 인터넷 방송 플랫폼 데이터 분석 프로젝트

## 📌 프로젝트 개요
본 프로젝트는 **인터넷 방송 플랫폼 데이터**를 수집하고 분석하는 것을 목표로 합니다. 
크롤링 기술을 활용하여 **치지직(CHZZK), 숲(SOOP), 유튜브(YouTube)** 3가지 플랫폼의 데이터를 **1시간마다 자동 수집**하여 데이터베이스에 저장하고, 이를 활용하여 다양한 분석을 수행합니다.

## 🏗️ 소프트웨어 아키텍처
![Internet_broadcast_architecture drawio](https://github.com/user-attachments/assets/bb592807-d289-4574-a7e4-375ef46e0a2e)

## 🛠️ 개발 환경
- **프로그래밍 언어**: Python
- **데이터베이스**: PostgreSQL
- **스케줄링**: Apache Airflow
- **데이터 분석**: Apache Superset
- **실행 환경**: Docker

## 📊 데이터 수집 개요
### 📅 데이터 수집 기간 및 기준
- **치지직 (CHZZK)**
  - 기간: 2024/11/06 ~ 2024/12/06
  - 기준: 시청자 수 TOP 20 라이브 방송
- **숲 (SOOP)**
  - 기간: 2024/11/06 ~ 2024/12/06
  - 기준: 시청자 수 TOP 15 라이브 방송
- **유튜브 (YouTube)**
  - 기간: 2024/11/01 ~ 2024/12/01
  - 기준: 최신 인기 급상승 동영상 및 게임 인기 급상승 동영상

### ⏳ 데이터 수집 주기
- **1시간마다 자동 수집**

## 📈 데이터 분석 내용
### 🏆 치지직(CHZZK) & 숲(SOOP)
- **일별 시청자 수 추이 분석**
- **요일별 시청자 수 분석**
- **인기 채널 및 특정 채널 분석**
- **방송 카테고리별 시청자 수 분석**
- **치지직 vs. 숲 플랫폼 간 시청자 수 비교**

### 🎥 유튜브(YouTube)
- **일별 조회수 추이 분석**
- **요일별 조회수 분석**
- **인급동(인기 급상승 동영상) 도달 소요 시간 분석**
- **인기 채널 분석**

## 📊 프로젝트 결과 (대시보드)
### 📌 치지직(CHZZK) 분석 대시보드
#### 1️⃣ 시청자 수 분석 대시보드
![치지직-시청자-수-분석](https://github.com/user-attachments/assets/0c71d6ea-03a0-42fe-b669-224476c03b10)
- 2024/12/02에 최고 시청자 수 기록
- 일요일,토요일에 가장 높은 평균 시청자 수 기록

#### 2️⃣ 채널별 분석 대시보드
![치지직-채널-분석용-대시보드-2025-02-13T08-18-05 332Z](https://github.com/user-attachments/assets/7e249299-29a5-48c4-9146-30443e238c10)
- 최고 시청자 수가 높은 채널: "아이리 칸나", "한동숙", "LCK", ...
- 평균 팔로워 수가 높은 채널: "한동숙", "랄로", "울프", ...
- 일별 TOP 1 시청자 수를 많이 등극한 채널: "한동숙", "울프", "LCK", ...
- 최고 시청자 수 도달까지 걸린 시간이 짧은 채널: "삼식123", "피닉스박", "강지", ...
- 채널별 최고 시청자 수 도달 정보 분석 가능
- 특정 채널(예: 풍월량) 상세 분석 가능
  - ![치지직-특정-채널-분석](https://github.com/user-attachments/assets/4bf8cc1f-d5e3-47f9-af96-cdde11106508)

#### 3️⃣ 특정 채널 분석 (침착맨 예시)
![치지직-침착맨-분석](https://github.com/user-attachments/assets/1f37f6b6-d341-4b69-9ae2-365c2a664b2e)
- 최고 시청자 수 도달 날짜: 2024/11/18 & 2024/11/30
- 최고 시청자 수 도달 정보 예시:
  - 📌 도달 날짜 및 시각: 2024-11-18 15:00
  - 👀 시청자 수: 7,038명
  - 🎬 방송 제목: "거성 박명수 초대석"
  - 🎭 카테고리: talk"
  - 🚫 성인 방송 여부: false

#### 4️⃣ 카테고리별 분석 대시보드
![치지직-카테고리-분석](https://github.com/user-attachments/assets/0257ab38-29b4-47cd-b955-2c1f0a3a4fdb)
- 누적 시청자 수가 높은 카테고리: "League_of_Legends", "Grand_Theft_Auto_V", "talk", ...
- "League_of_Legends"는 2024/11/23과 2024/12/01에 최고 시청자 수 달성

### 📌 숲(SOOP) 분석 대시보드
#### 1️⃣ 시청자 수 분석 대시보드
![숲-시청자-수-분석](https://github.com/user-attachments/assets/3f543f53-ee0c-4c6f-b385-d5607e364104)
- 2024/11/23에 최고 시청자 수 기록
- 일요일, 토요일에 가장 높은 평균 시청자 수 기록

#### 2️⃣ 채널별 분석 대시보드
![숲-채널-분석](https://github.com/user-attachments/assets/ec1f929a-9cfd-47ea-9e13-b67a4ad33e50)
- 평균 시청자 수가 높은 채널: "고전파0", "t1esports", "봉준", ...
- 일별 TOP 1 시청자 수를 많이 등극한 채널: "봉준", "고전파0", "우왁굳", ...
- 특정 채널(예: 고전파0) 상세 분석 가능
  - ![숲-채널-분석용-대시보드-2025-02-13T07-14-30 047Z](https://github.com/user-attachments/assets/daddf9c0-8fe9-412f-9e86-9a6f2e0b5ce6)

#### 3️⃣ 카테고리별 분석 대시보드
![숲-카테고리-분석용-대시보드-2025-02-13T07-17-14 470Z](https://github.com/user-attachments/assets/b463edd5-80a6-4cbc-96c5-156e4a574359)
- 누적 시청자 수가 높은 카테고리: "마인크래프트", "토크/캠방", "리그 오브 레전드", ...
- "리그 오브 레전드"는 2024/11/23에 최고 시청자 수 달성

### 📌 치지직(CHZZK) vs. 숲(SOOP) 비교 분석 대시보드
![치지직-vs-숲-비교-분석용-대시보드-2025-02-13T09-06-30 565Z](https://github.com/user-attachments/assets/513b17e9-26ef-4eab-a261-947789041056)
- 시청자 수: 치지직 < 숲
- 두 플랫폼 모두 공통적으로 일요일,토요일에 가장 높은 평균 시청자 수 기록
- 두 플랫폼 모두 공통적으로 리그오브레전드, 토크 카테고리가 인기가 많음

### 📌 유튜브(YouTube) 분석 대시보드
#### 1️⃣ 최신 인급동(인기 급상승 동영상) 분석 대시보드
![유튜브-최신-인급동-분석용-대시보드-2025-02-13T07-24-05 324Z](https://github.com/user-attachments/assets/eba635a2-b982-4200-a025-a5d43039457f)
- 일반적으로 조회수와 구독자 수가 비례하는 모습을 보여 줌
- 일요일, 목요일, 금요일에 가장 높은 평균 조회수 기록
- 최신 인급동 도달까지 평균 34시간 소요

#### 2️⃣ 게임 인급동(인기 급상승 동영상) 분석 대시보드
![유튜브-게임-인급동-분석용-대시보드-2025-02-13T07-28-03 277Z](https://github.com/user-attachments/assets/dd6e056a-d5fd-4237-a88b-78f9349dfd9e)
- 일반적으로 조회수와 구독자 수가 비례하는 모습을 보여 줌
- 목요일, 금요일, 수요일에 가장 높은 평균 조회수 기록
- 최신 인급동 도달까지 평균 9시간 소요

#### 3️⃣ 최신 인급동(인기 급상승 동영상) 채널별 분석 대시보드
![유튜브-최신-인급동-채널별-분석용-대시보드-2025-02-13T07-29-44 838Z](https://github.com/user-attachments/assets/8eb0bfd1-49b5-41c1-9880-d5eb0d69ad3f)
- 평균 조회수가 높은 채널: "ROSÉ", "MrBeast", ...
- 평균 구독자수가 높은 채널: "MrBeast", "BANGTANTV", ...
- 인급동 개수가 많은 채널: "인생84", "channel fullmoon", "SMTOWN", "뜬뜬 DdeunDdeun", ...
- 특정 채널("인생84") 상세 분석 가능
  - ![유튜브-최신-인급동-채널별-분석용-대시보드-2025-02-13T07-35-54 891Z](https://github.com/user-attachments/assets/56f7785e-7d43-4f9e-8782-2c6e5a99a60f)

#### 4️⃣ 게임 인급동(인기 급상승 동영상) 채널별 분석 대시보드
![유튜브-게임-인급동-채널별-분석용-대시보드-2025-02-13T07-36-35 118Z](https://github.com/user-attachments/assets/08f227f1-0319-4560-a621-7d1fc34befe3)
- 평균 조회수가 높은 채널: "DaFuq!?Boom!", "LCK", ...
- 평균 구독자수가 높은 채널: "DaFuq!?Boom!", "Clash of Clans", ...
- 인급동 개수가 많은 채널: "[ALTUBE] 김성현TV", "전쓰트의 게임 채널 Junsst", "[8LJAYWALKING] 2021 TFT World Champion", ...
- 특정 채널("LCK") 상세 분석 가능
  - ![유튜브-게임-인급동-채널별-분석용-대시보드-2025-02-13T07-39-03 513Z](https://github.com/user-attachments/assets/a94d0154-6e15-433e-ad11-369cfa4f94e7)
