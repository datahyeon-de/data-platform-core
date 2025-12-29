# 🚀 Real-time CRM Data Pipeline on Hybrid Home-Server

"From Bare-metal to Real-time Insights"

> Proxmox 기반의 홈서버 인프라 위에서 초당 3,000건 이상의 스파이크 트래픽을 처리하는 엔드투엔드 CRM 데이터 플랫폼입니다.

## 📌 Project Overview

본 프로젝트는 이커머스 환경의 유저 행동 로그를 실시간으로 수집하고, RFM(Recency, Frequency, Monetary) 및 **이탈 위험군(Churn Risk)** 을 분석하여 마케팅에 즉시 활용 가능한 상태로 정제하는 데이터 플랫폼 구축을 목표로 합니다.

- **기간**: 2025.12.29 ~ 2026.01.04 (1주일 집중 프로젝트)
- **핵심 성과**:
  - 6대의 Mini PC를 활용한 Hybrid(VM + K8s) 인프라 홈 서버 구축.
  - 저수준 트래픽부터 Spike(3K TPS) 상황까지 대응하는 가변형 파이프라인.
  - Delta Lake & dbt를 활용한 데이터 신뢰성 및 레이크하우스 아키텍처 구현.

## 🏗️ System Architecture

### Data Flow

- **Source**: data-generator가 생성하는 Spike 시나리오 기반 가상 로그.
- **Ingestion**: 3-Node Kafka Cluster (VM 기반 고가용성 보장).
- **Processing**: Spark on K8s (Structured Streaming & Batch).
- **Storage**: MinIO (Object Storage) + Delta Lake (ACID Transaction).
- **Modeling**: dbt를 활용한 PostgreSQL DW 데이터 마트 구축.
- **Monitoring**: Prometheus & Grafana를 통한 실시간 리소스 및 지표 관측.

## 📂 Repository Structure

| Repository | Description | Key Tech |
|------------|-------------|----------|
| `home-server-infra` | 기반 시설 및 K8s/Kafka 클러스터 제어 | Ansible, Proxmox, K8s, Kafka |
| `data-platform-core` | 비즈니스 로직 및 데이터 가공 엔진 | Spark, dbt, Airflow, Delta Lake |
| `data-generator` | 부하 테스트용 데이터 및 시나리오 생성 | Python, Faker, Redis |

## 🛠️ Tech Stack

- **Infrastructure**: Proxmox VE 8.x, Ubuntu 24.04, Kubernetes
- **Messaging**: Confluent Kafka, Zookeeper
- **Data Lake**: MinIO, Delta Lake
- **Processing**: Apache Spark 3.x, dbt-core
- **Orchestration**: Apache Airflow
- **Database**: PostgreSQL
- **Monitoring**: Prometheus, Grafana

## 🤖 AI Agent & Collaboration Rules

이 프로젝트는 AI Thought Partner(Gemini/Claude)와 협업하여 **문서 중심 개발(Spec-driven Development)** 을 수행합니다.

- **Commit Message**: `type: Verb Description #issue` (예: `feat: Add Spark streaming #12`)
- **Context Log**: 모든 주요 의사결정은 `docs/ai-context/`에 기록됩니다.
- **Definition**: 상세 스펙은 `프로젝트_정의서-#1.md`를 참조하십시오.

## 📈 Roadmap (1-Week)

- [x] Day 1-2: Proxmox VM & K8s Cluster Provisioning (Ansible)
- [ ] Day 3-4: Kafka Ingestion & Spark Streaming Pipeline Development
- [ ] Day 5-6: Delta Lake Integration & dbt Data Modeling
- [ ] Day 7: Spike Load Test & Optimization Report

## Contact

[hyeondata@gmail.com]

