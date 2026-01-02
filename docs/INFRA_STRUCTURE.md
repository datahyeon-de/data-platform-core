# Infra Structure Management

이 문서는 CRM 데이터 플랫폼 구축을 위한 Proxmox 기반 전체 VM 및 인프라 자원의 현황을 관리하는 마스터 리스트입니다. 모든 인프라 변경 사항은 이 파일에 최우선적으로 반영됩니다.

## 📊 최종 확정 인프라 리스트 (Inventory)
### Host Server
| 구분 | OS | CPU/Thread | Memory | Storage | Hostname | IP (192.168.0.x) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Host Server | Proxmox Debian | 6c/12t | 32GB | 1TB | server-01 | .101 |
| Host Server | Proxmox Debian | 6c/12t | 32GB | 1TB | server-02 | .102 |
| Host Server | Proxmox Debian | 8c/16t | 32GB | 1.5TB | server-03 | .103 |
| Host Server | Proxmox Debian | 8c/16t | 32GB | 1.5TB | server-04 | .104 |
| Host Server | Proxmox Debian | 8c/16t | 32GB | 1.5TB | server-05 | .105 |
| Host Server | Proxmox Debian | 8c/16t | 32GB | 1.5TB | server-06 | .106 |

### VM Inventory
| 구분 | VM ID | Hostname | IP (192.168.0.x) | Target Node | Template | 비고 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **기존** | 100 | `vm-bastion` | .10 | server-06 | - | Tailscale/Ansible Host |
| **기존** | 101 | `kafka-01` | .11 | server-01 | - | Broker 01 |
| **기존** | 102 | `kafka-02` | .12 | server-02 | - | Broker 02 |
| **기존** | 103 | `kafka-03` | .13 | server-03 | - | Broker 03 |
| **신규** | 104 | `minio-server` | .14 | server-04 | 901 | Object Storage (VM) |
| **신규** | 105 | `postgres-dw` | .15 | server-05 | 901 | Data Warehouse (VM) |
| **신규** | 106 | `infra-prom-01` | .16 | server-06 | 901 | Host Prometheus (VM) |
| **신규** | 300 | `k8s-master-01` | .30 | server-01 | 902 | Control Plane |
| **신규** | 301 | `k8s-worker-01` | .31 | server-02 | 902 | Spark Worker 01 |
| **신규** | 302 | `k8s-worker-02` | .32 | server-03 | 902 | Spark Worker 02 |
| **신규** | 303 | `k8s-worker-03` | .33 | server-04 | 902 | Airflow/Services |
| **신규** | 304 | `k8s-worker-04` | .34 | server-05 | 902 | CRM Data Gen Worker |
| **신규** | 305 | `k8s-worker-05` | .35 | server-06 | 902 | K8s Monitor (Grafana) |

## 🛠️ 네트워크 공통 설정
- **Gateway**: 192.168.0.1
- **Netmask**: /24
- **DNS**: 8.8.8.8, 8.8.4.4 (Netplan 영구 반영)
- **SSH User**: ubuntu (Bastion Public Key 주입 완료)

## 🏗️ 스토리지 구성
- **LVM**: 각 Proxmox 노드의 `local-lvm` (OS 및 데이터 실무)
- **NFS**: `nfs-storage` (템플릿 저장 및 초기 클론용)