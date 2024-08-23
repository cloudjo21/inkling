
# feature

- 특정 도메인의 아이템을 추천할 때 feature는 아이템의 특성을 나타낸다.
- feature는 보통 추론 시점을 기준으로 정해진다.
  - 현재 시점 혹은 최근에 관찰한 특정 값일 수도 있다.
  - 또는 특정 시점으로부터 일정 기간의 통계값일 수도 있다.
- feature는 정적이거나 동적이다.
  - 처음부터 스칼라 값인 정적 feature
    - feature 제공시점에 하나의 고정된 연속형/이산형 스칼라 값일 수 있다.
  - 추론 직전까진 다른 형태였다가, 추론 시점에 스칼라 값으로 변환되는 동적 feature
    - feature 제공 직전까진 벡터나 다른 구조의 데이터로 존재하다가, 추론 시점에 하나의 고정된 연속형/이산형 스칼라 값으로 변환되어 제공되는 feature일 수 있다. 
- entity 관점에 따라서도 feature를 구성이 나뉜다.
  - user/item/user-by-item

# feature store

## offline/online feature store

- 아이템 추천 모델 학습을 위한 데이터셋엔 아이템의 feature가 포함되어야 한다.

### offline feature store

- 아이템의 특성 별 feature를 event time(전체 기간 혹은 미리 정한 기간 내의 모든 시점) 내에서 매 시점별로 저장한다.

### online feature store

- 추론 시점을 기준으로 최근 시점의 아이템의 특성 별 feature를 저장하고, 이를 실시간 제공한다.

## offline/online feature 빌드

### offline feature 빌드

- offline feature 빌드는 dag-build-lake-features-* 파이프라인으로 관리
  - 목표: 특정 기간 내 여러 데이터 소스로부터 feature를 추출하고, 데이터셋을 만든다. 이 때 데이터 누출 <sup>data leakage</sup>가 없도록 한다.
  - 먼저, 미리 정한 기간 <sup>event time</sup> 내에서 단위 기간<sup>point-in-time</sup>마다 feature를 만들어둔다.
    - 이 과정은 feature 빌드 productization 개발의 lake 레벨 feature 빌드 단계에 속한다.
  - 미리 정한 기간 <sup>event time</sup>에 맞는 데이터셋을 point-in-time join을 활용한 데이터셋을 만든다.
    - 이 과정은 feature 빌드 productization 개발의 mart 레벨 feature/데이터셋 빌드 단계에 속한다.
    - event time 내에서 단위 기간<sup>point-in-time</sup>만큼 나아가면서 데이터셋을 만든다.
    - 2024년 8월부터 11월까지의 타겟 데이터셋을 만드는 예시로 살펴보자.
      - 모델은 매일 학습되고, 단위기간이 1주일이라고 가정
      - 2024년 8월 1~7일의 feature를 만든다고 가정하면
        - 2024년 7월 30일 이전의 로그와 데이터로 단위기간 내 feature를 만든다
    - 단위 기간을 줄이거나 늘릴 수 있다.
      - 컴퓨팅 환경과 비용에 여유가 있다면, 최신성을 더욱 잘 반영하도록 단위기간을 줄일 수 있다.
    - feature 빌드 task 개발 과정에 point-in-time join을 구현하도록 상위 컴포넌트를 지원한다.
      - feature merger
      - feature 빌드 task (build_feature_*.py) 개발은 개별 feature마다 개발하게 된다.
        - 이 때,개별 feature마다 point-in-time join 구현을 FeatureMerger 하위 모듈에 개발한다.

### online feature 빌드
- 추천할 아이템의 최신 feature를 갖도록 빌드
- NoSQL 특성을 일부 갖는 검색엔진을 활용
  - 항상 최신의 feature를 제공하도록 색인 빌드 관리

# feature 개발 과정

## feature 분석 및 적재 개발
- ipython 노트로 데이터 분석 및 feature 빌드와 적재 과정 개발
- 서브프로젝트 kwazii에서 개발한다

## feature 빌드 productization 개발
- airflow DAG로 pre-feature 빌드, feature 빌드, 데이터셋 빌드 단계를 개발
  - feature 빌드 과정이 airflow DAG 파이프라인에 관리될 수 있도록 개발
  - feature 빌드는 크게 두 단계로 나눔
    - user 혹은 item 사이드에서 바로 구해지는 feature 빌드 혹은 관계성이 있는 feature 빌드에 필요한 데이터 빌드
      - lake 레벨의 feature 빌드에서 진행
    - user와 item의 관계가 맺어지는 시점에서의 feature 빌드
      - mart 레벨의 데이터셋 빌드 단계에서 진행
- 서브프로젝트 inkling/squirt에서 개발한다

## feature 서빙 개발
- 빌드된 feature가 온라인 feature 서빙이 되도록 개발
  - 요청한 record가 존재하지 않거나, record의 feature 값 계산에 필요한 정보가 부족할 때의 기본 값 처리 등도 함께 개발
- 서브프로젝트 dashi/koshi에서 개발한다

