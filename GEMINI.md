# 🎯 CORE CONTEXT: 1-Week CRM Data Platform
- **Goal**: Demonstrate DE skills via high-load (Spike) handling and hybrid infra setup.
- **Tech Stack**: Kafka(3-Node VM), Spark on K8s, Delta Lake, MinIO, dbt, PostgreSQL, Airflow, Prometheus/Grafana
- **Infrastructure**: 6 Mini-PCs (Proxmox) | 1 Desktop (Ubuntu) | K8s Cluster | Proxmox Host/VM Management via Ansible
- **Network**: Private (192.168.0.0/24)
- **Repositories**: `home-server-infra` (Base), `data-platform-core` (Logic), `data-generator` (Source)

# 📜 SHARED RULES

## 커밋 메시지 규칙
- **형식**: `type: Verb description #issue`
- **Type**: feat, fix, docs, perf, refactor, chore, test (소문자)
- **Verb**: 대문자로 시작하는 명령형 동사 (예: Add, Fix, Update, Remove)
- **예시**: `feat: Add Spark streaming pipeline #12`, `fix: Correct Kafka consumer lag handling #15`
- **상세**: 첫 줄 이후 빈 줄을 두고 본문에 변경 이유와 영향 범위 작성

## 데이터 파이프라인 흐름
**Logic Flow**: `data-generator` -> `Kafka` -> `Spark` -> `Delta Lake` -> `dbt` -> `Dashboard`

## 문서 참조
- 항상 `docs/ProejctRFC-#1.md` (프로젝트 정의서) 참조
- 작업 전 `docs/ai-context/` 최신 파일 확인 필수
- 기술 결정 사항은 `docs/ai-context/issue-[번호]-[제목].md` 형식으로 기록

## 제약 사항
- Proxmox/K8s 하이브리드 환경의 자원 제약 준수
- Spike 트래픽 (3K TPS) 처리 성능 최적화 우선순위
- 고가용성 (High Availability) 보장 필요

# 📑 CONTEXT SYNC PROTOCOL
1. Read `docs/ProejctRFC-#1.md` (프로젝트 정의서) and `docs/ai-context/` first.
2. Draft technical specs before coding.
3. Log the "Tiki-Taka" discussion into `docs/ai-context/` after completing tasks.
4. 레포지토리 간 의존성을 고려하여 변경 사항을 문서화하십시오.
5. **Git Commit**: 작업 종료 시 `git status` 및 `git diff`를 분석하고, `docs/ai-context/`의 이슈 파일(예: Issue-2-06.md)을 참조하여 메인 번호(`#2`)가 포함된 커밋 메시지를 생성하십시오.