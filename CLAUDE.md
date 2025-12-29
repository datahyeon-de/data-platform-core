# 🎯 CORE CONTEXT: 1-Week CRM Data Platform
- **Mission**: Build an E2E CRM pipeline in 1 week.
- **Tech Stack**: Kafka(3-Node VM), Spark on K8s, Delta Lake, MinIO, dbt, PostgreSQL, Airflow
- **Infrastructure**: 6 Mini-PCs (Proxmox) | 1 Desktop (Ubuntu) | K8s Cluster
- **Network**: Private (192.168.0.0/24)
- **Repositories**: `home-server-infra` (Base), `data-platform-core` (Logic), `data-generator` (Source)

# 🧠 CONTEXT RECOVERY (CRITICAL)
- 세션 시작 시 반드시 `docs/ai-context/`의 최신 파일을 읽어 중단된 지점부터 시작하십시오.
- 주요 의사결정은 `docs/ai-context/issue-[번호]-[제목].md` 형식으로 실시간 업데이트하십시오.
- `docs/ProejctRFC-#1.md` (프로젝트 정의서)를 먼저 확인하여 전체 맥락을 파악하십시오.

# 📜 SHARED RULES

## 커밋 메시지 규칙
- **형식**: `type: Verb description #issue`
- **Type**: feat, fix, docs, perf, refactor, chore, test (소문자)
- **Verb**: 대문자로 시작하는 명령형 동사 (예: Add, Fix, Update, Remove)
- **예시**: `feat: Add Spark streaming pipeline #12`, `fix: Correct Kafka consumer lag handling #15`
- **상세**: 첫 줄 이후 빈 줄을 두고 본문에 변경 이유와 영향 범위 작성

## 문서 참조
- 항상 `docs/ProejctRFC-#1.md` (프로젝트 정의서) 참조
- 작업 전 `docs/ai-context/` 최신 파일 확인 필수
- 기술 결정 사항은 `docs/ai-context/issue-[번호]-[제목].md` 형식으로 기록

## 제약 사항
- 모든 코드는 Proxmox/K8s 하이브리드 환경의 자원 제약을 준수해야 합니다.
- Spike 트래픽 (3K TPS) 처리 성능 최적화 우선순위
- 고가용성 (High Availability) 보장 필요

