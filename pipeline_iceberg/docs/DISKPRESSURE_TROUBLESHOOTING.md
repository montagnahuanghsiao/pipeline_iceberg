# Kubernetes DiskPressure 與 Gold Job 驅逐排查手冊

## 1. 文件目的

本文件適用於 Ocean Pipeline 執行大型西北太平洋 Gold 工作時，出現下列症狀：

- SSH 連線突然中斷。
- `ocean-gold` 顯示 `Error`、`Evicted` 或 `ContainerStatusUnknown`。
- `dtadm`、`dtm`、`dtw`、Frontend 等多個 Pod 同時被驅逐。
- Kubernetes Node 曾出現 `DiskPressure=True`。
- Pod exit code 為 `137`，但沒有 Java OOM stack trace。
- `kubectl logs` 或 `kubectl logs --previous` 無法取得舊 container log。

本問題的典型根因不是 Gold Driver 本身寫滿磁碟，而是 YARN NodeManager 的 local cache、Spark shuffle/spill、container log 與各 Kubernetes node 的 containerd 共用系統碟，導致可用空間低於 kubelet 的 ephemeral-storage 驅逐門檻。

---

## 2. 已確認的典型故障特徵

事件範例：

```text
Reason: Evicted
The node was low on resource: ephemeral-storage.
Threshold quantity: 9463104068
available: 9233784Ki
```

代表：

```text
kubelet 驅逐門檻：約 9.46 GB
故障時可用空間：約 9.2 GiB
```

即使 Pod 同時出現：

```text
Exit Code: 137
ContainerStatusUnknown
```

只要 Pod 的 `Reason` 與 `Message` 明確指出 `Evicted`、`ephemeral-storage`，就應優先判定為磁碟壓力，不應誤判為 JVM heap OOM。

Gold Driver 往往只使用約 1 MiB ephemeral-storage；真正的大量空間可能來自 `dtw-*` Pod 內的：

```text
/home/bigred/yarn/usercache
/home/bigred/yarn/filecache
Spark shuffle / spill
/var/lib/containerd
```

---

## 3. 事故發生時不要立即做的事

在保留證據前，不要立刻：

- 重跑 Gold。
- 重啟整個 Hadoop/YARN 叢集。
- 執行 `podman system prune -a`。
- 清除仍在執行中的 YARN application cache。
- 刪除 HDFS DataNode 目錄。
- 因為看到 exit code 137 就直接增加 Java heap。

先保存以下資訊：

```bash
kubectl get pod -n dt -o wide

kubectl get events -A \
  --sort-by='.lastTimestamp' |
tail -100

kubectl describe job ocean-gold -n dt

kubectl describe pod \
  "$(kubectl get pod -n dt -l job-name=ocean-gold -o jsonpath='{.items[0].metadata.name}')" \
  -n dt
```

如果 container 已消失，`kubectl logs` 失敗是正常現象。此時使用 OPS 落盤日誌：

```bash
tail -200 \
  "$(ls -1t /opt/zfs/project/logs/gold_batch_*.log | head -1)"
```

---

## 4. 逐步排查流程

### 4.1 確認 Node 狀態

```bash
kubectl get nodes \
  -o custom-columns='NODE:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status,DISK:.status.conditions[?(@.type=="DiskPressure")].status,MEMORY:.status.conditions[?(@.type=="MemoryPressure")].status,PID:.status.conditions[?(@.type=="PIDPressure")].status'
```

即使目前 `DISK=False`，也要檢查事件。kubelet 可能已經透過驅逐 Pod 自動回收空間，導致目前狀態看似正常。

### 4.2 從事件確認驅逐原因

```bash
kubectl get events -A \
  --sort-by='.lastTimestamp' |
grep -E 'DiskPressure|Evicted|ephemeral-storage|EvictionThresholdMet|FreeDiskSpaceFailed' |
tail -100
```

查看所有失敗 Pod 的原因：

```bash
kubectl get pod -n dt \
  --field-selector=status.phase=Failed \
  -o custom-columns='POD:.metadata.name,NODE:.spec.nodeName,REASON:.status.reason,MESSAGE:.status.message'
```

### 4.3 在最外層 Podman 主機檢查系統碟

必須在承載 `tkdt-*` Podman containers 的最外層主機執行，不是在 `dtadm` 內執行：

```bash
df -h /
df -ih /
sudo podman system df
sudo podman ps -a --size
```

建議系統碟至少保留 15～20 GB。若只剩約 10 GB，極易再次觸發 kubelet eviction。

### 4.4 檢查各 Kubernetes node 的 containerd

```bash
for node in \
  tkdt-control-plane \
  tkdt-master1 \
  tkdt-worker1 \
  tkdt-worker2 \
  tkdt-worker3; do

  echo "=== $node ==="

  sudo podman exec "$node" sh -c '
    du -sh \
      /var/lib/containerd \
      /tmp \
      /var/log \
      2>/dev/null
  '
done
```

如果某個 worker 的 `/var/lib/containerd` 明顯較大，應繼續檢查該 node 上的 Pod 與 YARN cache。

### 4.5 檢查 YARN local cache

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  echo "=== $pod ==="

  kubectl exec -n dt "$pod" -- \
    bash -lc '
      if [ -d /home/bigred/yarn ]; then
        du -h -d 2 /home/bigred/yarn 2>/dev/null |
        sort -h |
        tail -20
      else
        echo "YARN_DIR_NOT_CREATED"
      fi
    '
done
```

曾實際觀察到：

```text
dtw-0 /home/bigred/yarn ≈ 3.3 GiB
旧 dtw-1 ephemeral-storage ≈ 9.2 GiB
```

這類暫存量足以讓約 59 GB 的系統碟跌破 kubelet 驅逐門檻。

### 4.6 檢查孤兒 YARN Application

Kubernetes Gold Driver 被驅逐後，YARN Application 可能仍在執行：

```bash
yarn application -list -appStates RUNNING
```

若確認是已失去 Driver 的 Gold Application：

```bash
yarn application -kill application_xxx
```

不要清除仍在執行中的 Application cache。

---

## 5. 臨時恢復步驟

### 5.1 停止 YARN

確認沒有有效 Running Application 後：

```bash
stopyarn
```

### 5.2 清除已失敗工作的 YARN cache

保留 `/home/bigred/yarn` 根目錄，只刪除其內容：

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  kubectl exec -n dt "$pod" -- \
    bash -lc '
      if [ -d /home/bigred/yarn ]; then
        find /home/bigred/yarn \
          -mindepth 1 \
          -maxdepth 1 \
          -exec rm -rf -- {} +
      fi
    '
done
```

### 5.3 安全清理本機建置快取

```bash
sudo podman image prune
sudo podman builder prune
sudo podman system df
```

不要在尚未確認 image 使用情況前執行：

```bash
sudo podman system prune -a
```

### 5.4 清除 Evicted Pod 紀錄

```bash
kubectl delete pod -n dt \
  --field-selector=status.phase=Failed
```

這主要清理 Kubernetes API 物件，本身不會釋放大量磁碟。

### 5.5 確認恢復空間

```bash
df -h /

kubectl get nodes \
  -o custom-columns='NODE:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status,DISK:.status.conditions[?(@.type=="DiskPressure")].status'
```

验收条件：

- 系統碟可用空間建議至少 15～20 GB。
- 所有 Node `READY=True`。
- 所有 Node `DISK=False`。

---

## 6. HDFS 與 YARN 恢復驗收

先確認 Kubernetes Pod：

```bash
kubectl get pod -n dt -o wide
```

核心 Pod 應為：

```text
dtadm  1/1 Running
dtm-0  1/1 Running
dtm-1  1/1 Running
dtw-0  1/1 Running
dtw-1  1/1 Running
dtw-2  1/1 Running
```

進入 `dtadm` 後先檢查 JVM，不要重複啟動已經存在的服務：

```bash
hls
```

檢查 HDFS：

```bash
hdfs dfsadmin -report
hdfs dfs -ls /
```

验收：

```text
Live DataNodes: 3
Dead DataNodes: 0
```

檢查 YARN：

```bash
startyarn
yarn node -list -all
```

驗收：3 個新 NodeManager 均為 `RUNNING`。

---

## 7. 只重跑失敗的西北太平洋 Gold

若日誌已明確顯示臺灣成功，例如：

```text
GOLD aoi=taiwan status=success
GOLD aoi=northwest_pacific stage=backbone ... status=ready
```

恢復後可只重跑西北太平洋，避免重複計算臺灣：

```bash
cd /opt/zfs/project

AOI_IDS=northwest_pacific \
bash pipeline_iceberg/ops/run_gold_batch.sh \
  2021 02
```

完成後仍會繼續執行 Gold Dashboard、Serving export 與批次驗證。

---

## 8. OPS 等待失敗 Job 的優化

目前腳本只等待：

```text
condition=complete
```

若 Job 已進入 Failed，可能繼續等到 `GOLD_TIMEOUT=12h`。事故時若終端仍卡在：

```text
kubectl wait --for=condition=complete
```

可先按 `Ctrl+C`，完成磁碟修復後再重跑。

建議後續將 `run_gold_batch.sh` 的等待邏輯優化為輪詢：

- `Complete=True`：成功。
- `Failed=True`：立即顯示 log/describe 並回傳失敗。
- 超過 timeout：回傳逾時。

這樣不必在已確定失敗後繼續等待 12 小時。

---

## 9. 永久修正：YARN local dirs 移至 ZFS

### 9.1 目標目錄

```text
/opt/zfs/yarn/dtw-0/local
/opt/zfs/yarn/dtw-0/logs
/opt/zfs/yarn/dtw-1/local
/opt/zfs/yarn/dtw-1/logs
/opt/zfs/yarn/dtw-2/local
/opt/zfs/yarn/dtw-2/logs
```

在最外層主機建立：

```bash
sudo mkdir -p \
  /opt/zfs/yarn/dtw-0/{local,logs} \
  /opt/zfs/yarn/dtw-1/{local,logs} \
  /opt/zfs/yarn/dtw-2/{local,logs}

sudo chown -R 1000:1001 /opt/zfs/yarn
sudo chmod -R 2775 /opt/zfs/yarn
```

### 9.2 dtw StatefulSet 挂载

先备份：

```bash
kubectl get statefulset dtw -n dt \
  -o yaml \
  > /tmp/dtw-before-yarn-zfs.yaml
```

在 dtw container 新增：

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name

volumeMounts:
  - name: yarn-local
    mountPath: /home/bigred/yarn
    subPathExpr: $(POD_NAME)
```

在 Pod spec 新增：

```yaml
volumes:
  - name: yarn-local
    hostPath:
      path: /opt/zfs/yarn
      type: Directory
```

`subPathExpr` 會自動隔離：

```text
dtw-0 → /opt/zfs/yarn/dtw-0
dtw-1 → /opt/zfs/yarn/dtw-1
dtw-2 → /opt/zfs/yarn/dtw-2
```

三個 NodeManager 不可共用同一個 local directory。

### 9.3 yarn-site.xml

```xml
<property>
  <name>yarn.nodemanager.local-dirs</name>
  <value>/home/bigred/yarn/local</value>
</property>

<property>
  <name>yarn.nodemanager.log-dirs</name>
  <value>/home/bigred/yarn/logs</value>
</property>

<property>
  <name>yarn.nodemanager.localizer.cache.cleanup.interval-ms</name>
  <value>600000</value>
</property>

<property>
  <name>yarn.nodemanager.localizer.cache.target-size-mb</name>
  <value>1024</value>
</property>

<property>
  <name>yarn.nodemanager.log.retain-seconds</name>
  <value>3600</value>
</property>
```

### 9.4 挂载验收

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  echo "=== $pod ==="
  kubectl exec -n dt "$pod" -- df -h /home/bigred/yarn
done
```

`/home/bigred/yarn` 必須顯示 ZFS 容量，不應再顯示約 59 GB 的 overlay filesystem。

---

## 10. 執行大型 Gold 時的監控

系統碟：

```bash
watch -n 10 'df -h /'
```

Node DiskPressure：

```bash
watch -n 15 'kubectl get nodes -o custom-columns="NODE:.metadata.name,DISK:.status.conditions[?(@.type==\"DiskPressure\")].status"'
```

YARN cache：

```bash
watch -n 30 '
for pod in dtw-0 dtw-1 dtw-2; do
  echo "=== $pod ==="
  kubectl exec -n dt "$pod" -- du -sh /home/bigred/yarn 2>/dev/null
done
'
```

建議告警門檻：

- 系統碟可用空間小於 20 GB：Warning。
- 系統碟可用空間小於 15 GB：停止提交新的大型 Job。
- 任一 Node `DiskPressure=True`：立即停止批次並檢查 cache。

---

## 11. 事故结论模板

```text
問題類型：Kubernetes ephemeral-storage DiskPressure
直接原因：系統碟可用空間低於 kubelet eviction 門檻
主要來源：YARN usercache / Spark shuffle / containerd writable layers
Gold 狀態：臺灣成功；西北太平洋未完成
Exit 137：本次為 eviction，不是 JVM OOM
臨時修復：停止孤兒 application、清理失敗工作 cache、恢復磁碟空間
永久修復：每臺 dtw 使用獨立 ZFS YARN local/log directory
重跑策略：僅重跑失敗的 northwest_pacific AOI
```
