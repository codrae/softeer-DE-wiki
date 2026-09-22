# W1M2 - SQL Tutorial

w3schools 샘플 데이터베이스(`w3schools.db`)를 Python의 `sqlite3`로 열어 SQL 기본 구문을
실습한 미션입니다. (`sql.ipynb`)

## sqlite3를 쓰는 이유

보통 SQL은 데이터베이스 **서버**(MySQL, PostgreSQL 등)에 접속해 실행한다. 반면 SQLite는
서버 없이 **하나의 `.db` 파일만으로** 데이터를 저장한다. `sqlite3`는 Python과 SQLite를
연결해 주는 인터페이스 역할을 한다. 별도 설치·기동 없이 바로 쿼리를 실습할 수 있어
튜토리얼 용도에 적합하다.

## sqlite3 동작 과정

1. **데이터베이스 연결** — `sqlite3.connect(...)`
2. **Cursor 생성** — Cursor는 SQL을 실행하는 객체
3. **SQL 실행** — `cur.execute(...)`
4. **결과 가져오기** — `cur.fetchall()`은 직전에 실행한 SQL의 결과를 전부 한 번에 가져온다
5. **Connection 관리** — Commit / Rollback / Close

## 새로 알게 된 · 헷갈렸던 구문

| 구문 | 메모 |
|---|---|
| `OFFSET` | `LIMIT`과 함께 쓰는 건너뛰기. 페이지네이션에 사용 |
| `AS [ ]` | 공백이 포함된 별칭을 대괄호로 감쌈 |
| `SELECT INTO` | 조회 결과를 새 테이블로 복사 |
| `LIKE '[bsp]%'` / `LIKE '[a-f]%'` | 문자 집합·범위 패턴 매칭 |

## 실행

```bash
jupyter lab sql.ipynb
```

> `w3schools.db`는 `.gitignore` 대상입니다. w3schools의 샘플 DB를 내려받아 같은 경로에 두고 실행하세요.
