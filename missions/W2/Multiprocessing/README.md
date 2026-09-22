# W2 M1~M4 - Python Multiprocessing

Python `multiprocessing` 모듈의 세 가지 축(Pool / Process / Queue)을 단계적으로 익히고,
마지막에 이를 하나로 합친 워커 패턴을 구현하는 미션 묶음입니다.

| 미션 | 주제 | 파일 |
|---|---|---|
| [M1](M1_pool) | `Pool`로 워커 풀을 만들어 작업을 동시 실행 | `pool.py` ([요구사항](M1_pool/README.md)) |
| M2 | `Process`를 리스트로 관리하며 `start`/`join` | `M2_Process/process.py` |
| M3 | `Queue`를 공유 자원으로 사용 | `M3_Queue/queue.py` |
| M4 | Queue + Process를 합친 all-in-one 워커 패턴 | `M4_all-ain-one/multiprocessing_all_in_one.py` |

`practice.py`는 라이브러리 감을 잡기 위한 연습 파일입니다.

## 미션별 학습 포인트

**M1 (Pool)** — 워커 2개로 구성한 풀에 `work_log` 함수를 `map`으로 할당. 작업이 워커 수만큼
동시에 진행되면서, 짧은 작업이 긴 작업보다 먼저 끝나는 출력 순서를 확인했다.

**M2 (Process)** — 프로세스를 리스트에 모아 두고 전부 `start()` 한 뒤 전부 `join()`하는 패턴.
`args`에 빈 튜플 `()`을 넘기면 함수의 **기본값 인자**가 그대로 쓰인다는 점을 팀 리뷰에서 반영해,
`[(), ("America",), ("Europe",), ("Africa",)]` 형태로 인자를 구성했다.

**M3 (Queue)** — `multiprocessing.Queue`가 표준 `queue.Queue`와 사용법이 거의 같다는 점,
그리고 `Pool`과 달리 **`close()`를 명시하지 않아도 되는 이유**를 함께 정리했다.

**M4 (all-in-one)** — 실행 대기 Queue와 완료 Queue를 분리해 공동 자원으로 사용하고,
`get()`과 `get_nowait()`의 차이(블로킹 여부, `queue.Empty` 처리)를 다뤘다. Queue를 전역으로
바로 참조하지 않고 **인자로 넘겨야 하는 이유**, 인자가 하나일 때 `args=(x,)`처럼 콤마를 붙여야
하는 이유도 주석으로 남겼다. 여러 프로세스가 동시에 출력할 때 줄바꿈이 섞이는 현상도 관찰했다.

## 주차 회고 (발췌)

> 기존에 어렴풋이 알고 사용했던 Python의 Multiprocessing을 더 깊이 다룰 수 있었다.
> Python에서 multi threading이 아닌 processing 라이브러리가 등장하게 된 배경부터
> `Pool`, `Queue`, `get_nowait()` 등의 사용법을 숙지할 수 있었다.

## 실행

```bash
python M1_pool/pool.py
python M2_Process/process.py
python M3_Queue/queue.py
python M4_all-ain-one/multiprocessing_all_in_one.py
```
