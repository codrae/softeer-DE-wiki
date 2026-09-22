# W5M2 - NYC TLC 분석 (Spark DataFrame & DAG)

[W5M1](../M1)과 동일한 NYC TLC 데이터를 **DataFrame API**로 재구현하고,
DAG·lazy evaluation·스테이지 최적화를 분석하는 미션입니다.

> **분석 결과 전문은 [REPORT.md](REPORT.md)를 참고하세요.** 이 문서는 미션 요구사항과
> 실행 방법을 다룹니다.

## 미션 요구사항

### 학습 목표
Spark 내부 동작 — 특히 **DataFrame과 DAG(Directed Acyclic Graph)** 개념을 이해한다.

### 기능 요구사항

| 항목 | 요구 내용 |
|---|---|
| Data Loading | TLC 데이터를 DataFrame으로 로드. 스키마는 추론하거나 명시적으로 정의 |
| Data Cleaning | 유효하지 않거나 null인 항목 제거, 비현실적 값 필터링 |
| Transformations | filtering / aggregation / join 등 다양한 변환으로 인사이트 도출. **명확하고 최적화된 DAG**가 되도록 설계 |
| Actions | 변환을 트리거할 action 실행, 결과를 지정 포맷으로 저장 |
| Optimization | 스테이지 수를 최소화하고 caching/persisting을 효과적으로 사용해 실행 계획 최적화 |
| Documentation | **리포트 작성** — 만들어진 DAG, 변환/액션 단계, Spark 내부 메커니즘(lazy evaluation, 스테이지 최적화)을 어떻게 활용했는지 설명 |

### 프로그래밍 요구사항
- 필수 컬럼(pickup/dropoff 시각, 승객 수, 이동거리)의 null·비정상 행 제거
- **최소 3가지 변환** — Filtering(예: 승객 2명 이상), Aggregation(총 트립수·평균 거리·총 매출),
  Join(다른 데이터셋과 결합)
- **최소 2가지 action** — Collect(샘플을 드라이버로 수집), Write(Parquet/CSV 저장)
- `cache()` / `persist()`로 반복 연산 최적화, 스테이지 수 최소화
- **Lazy evaluation 시연** — action이 호출되기 전까지 변환이 실행되지 않음을 보일 것
- Spark UI의 **DAG 시각화 스크린샷**을 리포트에 포함

## 구현 요약 (`main.py`)

```
로딩(parquet + zone lookup CSV)
  → 클리닝 (필수 컬럼 null 제거, 파생 컬럼, 이상치 필터)
  → cache() + count()로 한 번에 materialize
  → 4갈래 분기
      ├─ Filtering:   passenger_count > 1 다인승 트립
      ├─ Aggregation: 일별 트립수/평균거리/총매출
      ├─ Aggregation: 시간대별 트립수
      └─ Join:        broadcast(zone_lookup)로 borough별 집계
  → Actions: collect() 샘플, write() parquet + CSV
```

**핵심 결과** (자세한 해석은 [REPORT.md](REPORT.md))
- 원본 4,090,836행 → 정제 후 3,021,419행 (26.14% 드롭)
- `explain()`은 실행 계획만 출력하고 잡을 트리거하지 않음을 타임스탬프 로그로 확인
- Spark UI의 "(skipped)" 스테이지가 **셔플 출력 재사용**과 **`cache()` 효과**라는
  서로 다른 두 최적화의 결과임을 구분해 해석
- 265행짜리 zone lookup을 `broadcast()`로 감싸 큰 테이블 셔플 제거

## 학습 정리 (W5 강의 범위)

미션과 함께 정리한 DataFrame/DAG 관련 개념입니다.

**Spark Read & Schema** — 스키마 자동 추론 vs 명시, CSV와 Parquet의 스키마 내장 여부 차이,
Schema on Read 개념. 탐색적 분석에서는 추론, 프로덕션 ETL에서는 명시가 유리하며,
Structured Streaming은 스키마 지정이 필수다.

**Caching & Persistence** — `.cache()` vs `.persist()`(StorageLevel: MEMORY_ONLY,
MEMORY_AND_DISK 등), `persist()` vs `checkpoint()`. Eager Caching 패턴에서 `.count()`를
뒤에 붙이는 이유(lazy → 강제 materialize)와, `.take()`를 쓰면 일부 파티션만 캐싱될 수 있다는
주의점. 캐싱은 파티션(block) 단위로 이뤄진다.

**Join** — SQL 관점(inner/outer/left/right, left semi·anti, cross, natural)과
물리 전략(BroadcastHashJoin / ShuffleHashJoin / SortMergeJoin). AQE가 SortMergeJoin을
BroadcastHashJoin으로 동적 전환하며, `broadcast()` 명시는 통계 기반 자동 판단의 한계를
보완하지만 OOM 위험이라는 트레이드오프가 있다.

**Catalyst** — Parsed → Analyzed → Optimized → Physical Plan, `.explain()` 활용.

**Partitioning & Shuffle** — `coalesce` vs `repartition`(셔플 유무), 파티션 재배치가 필요한
연산(groupBy, join, distinct). AQE의 CoalesceShufflePartitions, OptimizeSkewedJoin,
OptimizeLocalShuffleReader.

## 실행

```bash
python main.py            # 실행 로그는 run.log 참고
pytest tests/ -v
```

> 입력 데이터(`data/`)는 `.gitignore` 대상입니다. TLC Yellow Taxi parquet과
> TLC 공식 Taxi Zone Lookup CSV를 내려받아 `data/`에 두세요.

## 설계 문서

- [설계(spec)](docs/superpowers/specs/2026-07-30-nyc-tlc-dataframe-dag-design.md)
- [구현 계획(plan)](docs/superpowers/plans/2026-07-30-nyc-tlc-dataframe-dag.md)

## 5주차 리뷰 피드백

> DataFrame 사용 시 query optimizing이 자동으로 켜져 있다.
> **이걸 꺼보고 이전과 비교해 보아야 한다.**

(AQE·Catalyst 최적화를 비활성화한 상태와 비교하는 실험은 아직 수행하지 않은 백로그입니다.)
