# Gold 月批次中斷事件 Runbook：2021_02 ocean-gold Pod Evicted

## 1. 事件摘要

本次執行 Gold 月批次自動化時，流程跑到 `2021_02` 後中斷。

執行指令：

```bash
cd /opt/zfs/project
bash pipeline_iceberg/ops/run_gold_monthly.sh 2021 02 12
```

觀察到的狀態：

```text
job.batch/ocean-gold     Failed
pod/ocean-gold-rkz75     Failed / Evicted / ContainerStatusUnknown
yarn RUNNING             0
```

判斷結果：

```text
不是 Spark 還在慢慢跑。
不是 Iceberg schema 或資料轉換邏輯錯。
是 Kubernetes node ephemeral-storage 壓力過高，導致 ocean-gold driver Pod 被 Evicted。
```

本次應從 `2021_02` 重新接續，不能直接從 `2021_03` 開始，因為 `2021_02 Gold` 沒有完整完成，`2021_02 Serving` 也沒有可靠完成。

---

## 2. 如何判斷 Job 是否卡住或已停止

### 2.1 查 Kubernetes Job / Pod

```bash
kubectl get job,pod -n dt | grep ocean
```

本次觀察到：

```text
job.batch/ocean-gold                 Failed
pod/ocean-gold-rkz75                 ContainerStatusUnknown
job.batch/ocean-serving-export       Complete
```

判讀：

```text
ocean-gold Failed = Gold 已經失敗停止
ContainerStatusUnknown = container 狀態已遺失，常見於 Evicted / node 壓力 / container 被清除
ocean-serving-export Complete = 最近曾有 Serving 成功，但不代表目前這個 Gold batch 完整完成
```

### 2.2 查 YARN 是否還有 Spark 任務在跑

```bash
yarn application -list -appStates RUNNING
```

本次觀察到：

```text
Total number of applications ... states: [RUNNING] ... :0
```

判讀：

```text
YARN 沒有正在跑的 Spark application。
因此不是還在運算，而是已停。
```

### 2.3 查目前 ConfigMap 停在哪個 batch

```bash
kubectl get configmap ocean-pipeline-config -n dt \
  -o jsonpath="{.data.BATCH_ID}{' '}{.data.START_DATE}{' '}{.data.END_DATE}{' '}{.data.SERVING_RELEASE_ID}{'\n'}"
```

本次觀察到：

```text
2021_02 2021-02-01 2021-02-28 2021_02
```

判讀：

```text
目前 pipeline 參數停在 2021_02。
下次續跑至少要從 2021_02 開始。
```

---

## 3. 排查 Gold Job 失敗原因

### 3.1 查看 Job 描述

```bash
kubectl describe job ocean-gold -n dt
```

本次重點：

```text
Pods Statuses: 0 Active / 0 Succeeded / 1 Failed
Node-Selectors: kubernetes.io/hostname=tkdt-worker3
```

判讀：

```text
ocean-gold 固定排到 tkdt-worker3。
如果 tkdt-worker3 出現磁碟壓力，Gold driver Pod 會被影響。
```

### 3.2 查看 Gold Pod 描述

```bash
kubectl describe pod ocean-gold-rkz75 -n dt
```

本次關鍵訊息：

```text
Status:  Failed
Reason:  Evicted
Message: The node was low on resource: ephemeral-storage.
Threshold quantity: 9463104068, available: 9233784Ki.
Container spark-submit was using 1016Ki, request is 0, has larger consumption of ephemeral-storage.

State:
  Reason: ContainerStatusUnknown
  Exit Code: 137
```

判讀：

```text
Kubernetes node ephemeral-storage 不足。
Pod 被 kubelet Evicted。
Exit Code 137 在這裡不是單純 JVM heap OOM，而是 Pod 被系統中止。
```

注意：

```text
Container spark-submit was using 1016Ki
```

不代表 Gold 只用了 1MB 空間。它代表：

```text
整個 node 已經低於 ephemeral-storage eviction threshold。
該 Pod 沒有設定 ephemeral-storage request，所以更容易被挑中 Evict。
```

---

## 4. 排查目前 Gold 進度

### 4.1 查看最新 monthly log

```bash
tail -80 $(ls -t /opt/zfs/project/logs/gold_monthly_*.log | head -1)
```

本次重點：

```text
2026-07-11T20:57:07Z MONTHLY status=starting year=2021 start_month=02 end_month=12
2026-07-11T20:57:07Z MONTHLY batch=2021_02 status=starting
2026-07-11T20:57:07Z GOLD status=starting batch=2021_02 start=2021-02-01 end=2021-02-28
GOLD aoi=taiwan status=starting
GOLD aoi=taiwan stage=backbone rows=774144 status=ready
GOLD aoi=taiwan status=success
GOLD aoi=northwest_pacific status=starting
GOLD aoi=northwest_pacific stage=backbone rows=11059200 status=ready
2026-07-11T21:39:20Z RUN kubectl wait -n dt --for=condition=complete job/ocean-gold --timeout=12h
```

判讀：

```text
2021_02 taiwan Gold 已成功。
2021_02 northwest_pacific 已完成 backbone，但尚未看到 status=success。
Job 沒有 complete，表示 2021_02 Gold 整體未完成。
```

### 4.2 查看最新 batch log

```bash
tail -120 $(ls -t /opt/zfs/project/logs/gold_batch_*.log | head -1)
```

快速搜尋錯誤：

```bash
grep -E "status=failed|FAILED|Error|Exception|ContainerStatusUnknown|Lost executor|OutOfMemory|Killed|OOM|Evicted|GOLD|SERVING|MONTHLY" \
  $(ls -t /opt/zfs/project/logs/gold_batch_*.log | head -1)
```

---

## 5. 確認目前應從哪裡重新開始

### 5.1 先確認 ConfigMap

```bash
kubectl get configmap ocean-pipeline-config -n dt \
  -o jsonpath="{.data.BATCH_ID}{' '}{.data.START_DATE}{' '}{.data.END_DATE}{' '}{.data.SERVING_RELEASE_ID}{'\n'}"
```

本次結果：

```text
2021_02 2021-02-01 2021-02-28 2021_02
```

### 5.2 檢查 Serving batch 是否完整

```bash
BATCH_ID=2021_02

hdfs dfs -count -h /dataset/ocean/serving/batches/$BATCH_ID/gold_map_metric
hdfs dfs -count -h /dataset/ocean/serving/batches/$BATCH_ID/gold_dashboard_daily_metrics
hdfs dfs -count -h /dataset/ocean/serving/batches/$BATCH_ID/gold_dashboard_status_distribution
```

判斷：

```text
如果三個路徑都有資料，代表該 batch Serving 有輸出。
但因為 ocean-gold Job 本身 Failed，不建議直接信任 2021_02 已完整。
本次建議從 2021_02 重跑。
```

### 5.3 本次建議續跑點

```text
從 2021_02 重新跑到 2021_12。
```

原因：

```text
2021_02 ocean-gold 沒 complete。
northwest_pacific 沒看到 status=success。
Serving 是否對應完整 Gold 結果不可保證。
Gold 設計應支援同 batch 重跑 / 覆蓋同 partition。
```

---

## 6. 根因：YARN local / log 目錄使用 ephemeral-storage

之前查到的設定：

```bash
grep -A2 -B1 \
  'yarn.nodemanager.local-dirs\|yarn.nodemanager.log-dirs' \
  /opt/zfs/sys/hadoop-3.5.0/etc/hadoop/yarn-site.xml
```

舊設定可能是：

```xml
<property>
  <name>yarn.nodemanager.local-dirs</name>
  <value>/home/bigred/yarn</value>
</property>
```

以及：

```text
yarn.nodemanager.log-dirs = /tmp/userlogs
```

問題：

```text
/home/bigred/yarn 和 /tmp/userlogs 容易吃到 Pod / node 的 ephemeral-storage。
Spark on YARN 的 container local cache、shuffle/spill、usercache、filecache、container logs 會快速累積。
Kubernetes 偵測 node ephemeral-storage 低於門檻後，會 Evict Pod。
```

---

## 7. 建議修正：把 YARN local / log 移到 dtw 各自 PVC

### 7.1 修改目標

將 YARN NodeManager local 與 log 目錄改成：

```xml
<property>
  <name>yarn.nodemanager.local-dirs</name>
  <value>/home/bigred/dn/yarn/local</value>
</property>

<property>
  <name>yarn.nodemanager.log-dirs</name>
  <value>/home/bigred/dn/yarn/logs</value>
</property>
```

原因：

```text
dtw StatefulSet 的 /home/bigred/dn 是每個 worker Pod 自己的 PVC。
把 YARN runtime 暫存移到這裡，可以降低 Kubernetes ephemeral-storage 壓力。
```

### 7.2 在 dtadm 修改 yarn-site.xml

```bash
nano /opt/zfs/sys/hadoop-3.5.0/etc/hadoop/yarn-site.xml
```

確認或加入：

```xml
<property>
  <name>yarn.nodemanager.local-dirs</name>
  <value>/home/bigred/dn/yarn/local</value>
</property>

<property>
  <name>yarn.nodemanager.log-dirs</name>
  <value>/home/bigred/dn/yarn/logs</value>
</property>
```

注意：

```text
XML 是放進 yarn-site.xml，不是貼到 bash 執行。
```

### 7.3 在每個 dtw Pod 建目錄

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  echo "=== $pod ==="
  kubectl exec -n dt "$pod" -- mkdir -p /home/bigred/dn/yarn/local /home/bigred/dn/yarn/logs
  kubectl exec -n dt "$pod" -- chown -R bigred:bigred /home/bigred/dn/yarn
  kubectl exec -n dt "$pod" -- chmod -R 755 /home/bigred/dn/yarn
done
```

確認：

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  echo "=== $pod ==="
  kubectl exec -n dt "$pod" -- df -h /home/bigred/dn
  kubectl exec -n dt "$pod" -- ls -ld /home/bigred/dn/yarn/local /home/bigred/dn/yarn/logs
done
```

### 7.4 重啟 YARN

```bash
stopyarn
startyarn
```

確認 NodeManager：

```bash
yarn node -list -all
```

預期：

```text
dtw-0 ... RUNNING
dtw-1 ... RUNNING
dtw-2 ... RUNNING
```

如果剛啟動看到 `Total Nodes:0`，等 10～30 秒再查一次。

### 7.5 驗證設定生效

```bash
hdfs getconf -confKey yarn.nodemanager.local-dirs
hdfs getconf -confKey yarn.nodemanager.log-dirs
```

預期：

```text
/home/bigred/dn/yarn/local
/home/bigred/dn/yarn/logs
```

---

## 8. 清理目前失敗狀態與舊 Pod

### 8.1 清 Gold / Serving Job

```bash
kubectl delete job ocean-gold -n dt --ignore-not-found
kubectl delete job ocean-serving-export -n dt --ignore-not-found
```

### 8.2 清 Completed / Failed Pod

先查看：

```bash
kubectl get pods -A | egrep 'Completed|Error|Evicted|ContainerStatusUnknown'
```

清 Completed：

```bash
kubectl delete pod -A --field-selector=status.phase=Succeeded
```

清 Failed：

```bash
kubectl delete pod -A --field-selector=status.phase=Failed
```

### 8.3 確認沒有殘留 Spark application

```bash
yarn application -list -appStates RUNNING
```

預期：

```text
Total number of applications ... :0
```

---

## 9. 檢查 DiskPressure / ephemeral-storage 狀態

### 9.1 查全部 node DiskPressure

```bash
kubectl get nodes \
  -o custom-columns='NODE:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status,DISK:.status.conditions[?(@.type=="DiskPressure")].status,MEMORY:.status.conditions[?(@.type=="MemoryPressure")].status,PID:.status.conditions[?(@.type=="PIDPressure")].status'
```

預期：

```text
DISK=False
```

### 9.2 查 tkdt-worker3 詳細資源

```bash
kubectl describe node tkdt-worker3 | grep -A20 -E "DiskPressure|ephemeral-storage|Allocated resources|Capacity|Allocatable"
```

### 9.3 查最近事件

```bash
kubectl get events -A \
  --sort-by='.lastTimestamp' | tail -50
```

若仍看到：

```text
EvictionThresholdMet
Evicted
ephemeral-storage
```

代表磁碟壓力尚未完全解除。

---

## 10. 可選優化：給 Gold / Serving Pod 設 ephemeral-storage request

目前 `04-gold-job.yaml` 只有：

```yaml
resources:
  requests:
    cpu: 500m
    memory: 1Gi
```

可以考慮補：

```yaml
resources:
  requests:
    cpu: 500m
    memory: 1Gi
    ephemeral-storage: 2Gi
  limits:
    cpu: "2"
    memory: 2Gi
    ephemeral-storage: 8Gi
```

較重的 Gold 可用：

```yaml
resources:
  requests:
    cpu: 500m
    memory: 1Gi
    ephemeral-storage: 4Gi
  limits:
    cpu: "2"
    memory: 2Gi
    ephemeral-storage: 12Gi
```

注意：

```text
這不是根本解法。
如果 node 實際可用 ephemeral-storage 真的不足，Pod 仍可能被 Evicted。
根本解法仍是把 YARN local/log 轉移到 PVC，並釋放 node 磁碟壓力。
```

---

## 11. 接續完成 Job 的指令

完成第 7～9 節調整與確認後，從 `2021_02` 接續：

```bash
cd /opt/zfs/project

kubectl delete job ocean-gold -n dt --ignore-not-found
kubectl delete job ocean-serving-export -n dt --ignore-not-found

yarn application -list -appStates RUNNING

bash pipeline_iceberg/ops/run_gold_monthly.sh 2021 02 12
```

即時追蹤 monthly log：

```bash
tail -f $(ls -t /opt/zfs/project/logs/gold_monthly_*.log | head -1)
```

即時追蹤目前單月 batch log：

```bash
tail -f $(ls -t /opt/zfs/project/logs/gold_batch_*.log | head -1)
```

查目前 ConfigMap batch：

```bash
kubectl get configmap ocean-pipeline-config -n dt \
  -o jsonpath="{.data.BATCH_ID}{' '}{.data.START_DATE}{' '}{.data.END_DATE}{' '}{.data.SERVING_RELEASE_ID}{'\n'}"
```

查目前 Kubernetes Job / Pod：

```bash
kubectl get job,pod -n dt | grep ocean
```

查 YARN running application：

```bash
yarn application -list -appStates RUNNING
```

---

## 12. 每個 batch 成功後的驗證

假設目前完成 `2021_02`：

```bash
BATCH_ID=2021_02

hdfs dfs -count -h /dataset/ocean/serving/batches/$BATCH_ID/gold_map_metric
hdfs dfs -count -h /dataset/ocean/serving/batches/$BATCH_ID/gold_dashboard_daily_metrics
hdfs dfs -count -h /dataset/ocean/serving/batches/$BATCH_ID/gold_dashboard_status_distribution
```

或使用 ops 驗證腳本：

```bash
cd /opt/zfs/project
bash pipeline_iceberg/ops/verify_gold_batch.sh 2021 02
```

若要確認前端 API 可見日期：

```bash
curl -sS http://ocean-frontend:8080/api/v1/catalog
curl -sS "http://ocean-frontend:8080/api/v1/available-dates?aoi=taiwan"
curl -sS "http://ocean-frontend:8080/api/v1/available-dates?aoi=northwest_pacific"
```

---

## 13. 下次遇到類似問題的最短判斷流程

```bash
kubectl get job,pod -n dt | grep ocean

yarn application -list -appStates RUNNING

kubectl get configmap ocean-pipeline-config -n dt \
  -o jsonpath="{.data.BATCH_ID}{' '}{.data.START_DATE}{' '}{.data.END_DATE}{' '}{.data.SERVING_RELEASE_ID}{'\n'}"

kubectl describe job ocean-gold -n dt

POD=$(kubectl get pod -n dt -l job-name=ocean-gold -o jsonpath='{.items[0].metadata.name}')
kubectl describe pod "$POD" -n dt

tail -80 $(ls -t /opt/zfs/project/logs/gold_monthly_*.log | head -1)
tail -120 $(ls -t /opt/zfs/project/logs/gold_batch_*.log | head -1)
```

判斷規則：

```text
YARN RUNNING = 0
且 ocean-gold Job = Failed
且 Pod Reason = Evicted
=> 已停止，不是卡住。

ConfigMap 的 BATCH_ID
=> 代表目前停在哪個批次。

若該批次 Gold job 沒有 Complete
=> 從該批次重跑。
```

