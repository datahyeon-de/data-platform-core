# Issue #1: 프로젝트 레포지터리 구성 및 AI 협업 환경 구축

## 📅 날짜: 2025-12-29
## 👤 참여자: 사용자, Gemini (AI Thought Partner)

## 💬 대화 요약
1. **프로젝트 정의 확정**: '고가용성 하이브리드 인프라 기반 실시간 CRM 데이터 파이프라인'으로 명칭 및 목표 확정.
2. **AI 환경 설정**: Cursor, Claude, Gemini가 동일한 맥락을 공유하도록 `.cursorrules`, `CLAUDE.md`, `GEMINI.md` 작성 및 배치 완료.
3. **규칙 수립**: 접두어(type)와 명령형 동사(Verb)를 조합한 엄격한 커밋 메시지 규칙 및 이슈 기반의 컨텍스트 관리 방식 수립.
4. **레포지토리 구조**: `home-server-infra`, `data-platform-core`, `data-generator` 3개 체제로 구성 완료.

## 🛠️ 결정된 사항
- 모든 작업은 이슈 생성 후 `docs/ai-context/`에 로그를 남기며 진행한다.
- 1주일 내 완성을 위해 인프라 구축(Day 1-2)부터 즉시 시작한다.

## 🔗 다음 작업
- Issue #2: [home-server-infra] Ansible을 이용한 Proxmox VM 프로비저닝 자동화