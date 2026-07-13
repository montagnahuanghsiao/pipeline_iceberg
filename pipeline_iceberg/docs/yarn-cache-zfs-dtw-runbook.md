# Runbook：將 dtw 節點的 YARN cache / logs 掛到 ZFS 獨立目錄

## 1. 目標

本流程目標是把 `dtw-0`、`dtw-1`、`dtw-2` 的 YARN NodeManager 暫存資料與 container logs 從 container writable layer / HDFS DataNode 目錄分離出來，改放到外層 ZFS-backed 目錄。

調整後的目標結構：

```text
# sea2 / 最外層 VM
/opt/zfs/tkdt/yarn/
├── dtw-0/
│   ├── local/
│   └── logs/
├── dtw-1/
│   ├── local/
│   └── logs/
└── dtw-2/
    ├── local/
    └── logs/

# dtw Pod 內
/home/bigred/yarn/local
/home/bigred/yarn/logs
```

用途：

- `/home/bigred/yarn/local`：YARN localized cache、application 暫存資料。
- `/home/bigred/yarn/logs`：YARN container logs。
- `/home/bigred/dn`：HDFS DataNode block storage，不能被覆蓋或清除。

這個調整可以降低大型 Spark / YARN 任務把暫存資料塞進錯誤位置，進而造成 container writable layer 爆掉、DiskPressure、NodeManager 異常或 HDFS DataNode 路徑混亂的風險。

---

## 2. 關鍵架構觀念

這套系統是巢狀架構：

```text
sea2 外層 VM
└── Podman container：tkdt-worker1 / tkdt-worker2 / tkdt-worker3
    └── Kubernetes node
        └── dtw-0 / dtw-1 / dtw-2 Pod
```

`kto/kcn` 建立 Podman Kubernetes node 時，會把外層：

```text
/opt/zfs/tkdt
```

掛進 Podman node 內：

```text
/opt/zfs
```

因此在 Kubernetes `hostPath` 寫：

```yaml
hostPath:
  path: /opt/zfs/yarn
```

實際上對應到 sea2 外層通常是：

```text
/opt/zfs/tkdt/yarn
```

這是本流程最容易看錯的一層。

---

## 3. 安全原則

整個流程中，嚴禁執行：

```bash
kubectl delete pvc -n dt dn-dtw-0
kubectl delete pvc -n dt dn-dtw-1
kubectl delete pvc -n dt dn-dtw-2
```

嚴禁執行：

```bash
formathdfs
```

嚴禁清除：

```text
/home/bigred/dn
```

原因：

- `dn-dtw-0/1/2` 是 HDFS DataNode 的 PVC。
- `/home/bigred/dn` 是 HDFS block 實際保存位置。
- `formathdfs` 會清掉 NameNode / DataNode 資料並重新 format HDFS，保留原資料時不能使用。

---

## 4. 前置檢查

### 4.1 確認沒有 YARN 任務在跑

在 `dtadm` 執行：

```bash
yarn application -list -appStates RUNNING,ACCEPTED,SUBMITTED
```

預期結果：

```text
Total number of applications ... :0
```

如果還有任務在跑，先不要修改 dtw StatefulSet。

---

### 4.2 停止 YARN

在 `dtadm` 執行：

```bash
stopyarn
```

確認 `NodeManager` 已停止：

```bash
hls
```

如果還不確定，逐台檢查：

```bash
for n in dtw-0 dtw-1 dtw-2; do
  echo "===== $n ====="
  ssh "$n" "jps -l"
done
```

`dtw-0/1/2` 不應該還有：

```text
org.apache.hadoop.yarn.server.nodemanager.NodeManager
```

---

## 5. 在 sea2 建立 ZFS-backed YARN 目錄

在 sea2，也就是最外層 VM 執行：

```bash
sudo mkdir -p \
  /opt/zfs/tkdt/yarn/dtw-0/local \
  /opt/zfs/tkdt/yarn/dtw-0/logs \
  /opt/zfs/tkdt/yarn/dtw-1/local \
  /opt/zfs/tkdt/yarn/dtw-1/logs \
  /opt/zfs/tkdt/yarn/dtw-2/local \
  /opt/zfs/tkdt/yarn/dtw-2/logs

sudo chown -R 1000:1001 /opt/zfs/tkdt/yarn
sudo chmod -R 2775 /opt/zfs/tkdt/yarn
```

驗證：

```bash
find /opt/zfs/tkdt/yarn \
  -maxdepth 2 \
  -type d \
  -printf '%M %u:%g %p\n'
```

如果 cluster name 不是 `tkdt`，請把：

```text
/opt/zfs/tkdt/yarn
```

改成：

```text
/opt/zfs/<cluster-name>/yarn
```

---

## 6. 修改 `sts-dtw.yaml`

檔案位置：

```bash
~/wulin/wk/dt/sts-dtw.yaml
```

編輯：

```bash
vi ~/wulin/wk/dt/sts-dtw.yaml
```

### 6.1 修改重點

在 `spec.template.spec.volumes` 加上：

```yaml
        - name: yarn-local
          hostPath:
            path: /opt/zfs/yarn
            type: Directory
```

在 container `dtw` 裡面加上：

```yaml
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
```

在 container `dtw` 的 `volumeMounts` 裡面加上：

```yaml
           - name: yarn-local
             mountPath: /home/bigred/yarn
             subPathExpr: $(POD_NAME)
```

注意，`subPathExpr` 必須寫：

```yaml
subPathExpr: $(POD_NAME)
```

不能寫成：

```yaml
subPathExpr: ${POD_NAME}
```

如果寫成 `${POD_NAME}`，Kubernetes 不會展開變數，會真的建立：

```text
/opt/zfs/tkdt/yarn/${POD_NAME}
```

導致三個 dtw Pod 全部共用同一個目錄。

---

### 6.2 完整 `sts-dtw.yaml` 範例

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: dtw
  namespace: dt
spec:
  selector:
    matchLabels:
      dt: worker
  serviceName: "svc-dt"
  volumeClaimTemplates:
  - metadata:
      name: dn
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: local-path
      resources:
        requests:
          storage: 200Gi
  replicas: 3
  template:
    metadata:
      labels:
       dt: worker
       app: dt
    spec:
      nodeSelector:
        dt: worker
      dnsPolicy: None
      dnsConfig:
        nameservers:
        - 10.98.136.10
        searches:
        - svc-dt.dt.svc.tkdt.k8s
        - dt.svc.tkdt.k8s
        - svc.tkdt.k8s
        - kube-system.svc.tkdt.k8s
      volumes:
        - name: dt-sys
          hostPath:
            path: /opt/zfs/sys
        - name: yarn-local
          hostPath:
            path: /opt/zfs/yarn
            type: Directory
      runtimeClassName: gvisor
      containers:
        - image: dkreg.taroko:5000/usdt.hdp350
          imagePullPolicy: Always
          name: dtw
          tty: true
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          volumeMounts:
           - name: dt-sys
             mountPath: /opt/zfs/sys
           - name: dn
             mountPath: /home/bigred/dn
           - name: yarn-local
             mountPath: /home/bigred/yarn
             subPathExpr: $(POD_NAME)
```

重點：

- `dn` 仍然掛在 `/home/bigred/dn`。
- `yarn-local` 掛在 `/home/bigred/yarn`。
- `subPathExpr: $(POD_NAME)` 會讓：

```text
dtw-0 -> /opt/zfs/yarn/dtw-0 -> /home/bigred/yarn
dtw-1 -> /opt/zfs/yarn/dtw-1 -> /home/bigred/yarn
dtw-2 -> /opt/zfs/yarn/dtw-2 -> /home/bigred/yarn
```

---

## 7. 檢查 YAML 並套用

先 dry-run：

```bash
kubectl apply --dry-run=client -f ~/wulin/wk/dt/sts-dtw.yaml
```

預期：

```text
statefulset.apps/dtw configured (dry run)
```

正式套用：

```bash
kubectl apply -f ~/wulin/wk/dt/sts-dtw.yaml
```

等待 rollout：

```bash
kubectl rollout status -n dt statefulset/dtw --timeout=10m
```

如果 Pod 沒有自動重建，可以刪除 Pod，讓 StatefulSet 重建：

```bash
kubectl delete pod -n dt dtw-0 dtw-1 dtw-2
kubectl get pod -n dt -w
```

注意：這裡是刪 Pod，不是刪 PVC。

---

## 8. 建立 local / logs 並驗證掛載

在 `dtadm` 執行：

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  echo "===== $pod ====="
  kubectl exec -n dt "$pod" -- bash -lc '
    mkdir -p /home/bigred/yarn/local /home/bigred/yarn/logs
    chmod -R 775 /home/bigred/yarn
    ls -ld /home/bigred/yarn /home/bigred/yarn/local /home/bigred/yarn/logs
  '
done
```

確認容量不是 overlay 小容量，而是 ZFS 對應容量：

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  echo "===== $pod ====="
  kubectl exec -n dt "$pod" -- df -h /home/bigred/yarn
  kubectl exec -n dt "$pod" -- bash -lc 'ls -ld /home/bigred/yarn /home/bigred/yarn/local /home/bigred/yarn/logs'
done
```

預期會看到類似：

```text
Filesystem      Size  Used Avail Use% Mounted on
none            386G  131G  255G  34% /home/bigred/yarn
```

---

## 9. 驗證三個 dtw 是否各自使用不同目錄

在 `dtadm` 寫入 owner：

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  kubectl exec -n dt "$pod" -- bash -lc 'hostname > /home/bigred/yarn/owner'
done
```

在 Pod 內確認：

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  echo -n "$pod: "
  kubectl exec -n dt "$pod" -- cat /home/bigred/yarn/owner
done
```

預期：

```text
dtw-0: dtw-0
dtw-1: dtw-1
dtw-2: dtw-2
```

在 sea2 外層確認：

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  echo -n "$pod: "
  cat "/opt/zfs/tkdt/yarn/$pod/owner"
done
```

預期：

```text
dtw-0: dtw-0
dtw-1: dtw-1
dtw-2: dtw-2
```

如果三台都顯示同一個值，例如：

```text
dtw-0: dtw-2
dtw-1: dtw-2
dtw-2: dtw-2
```

代表三個 Pod 實際共用同一個目錄，請檢查 `subPathExpr` 是否誤寫成：

```yaml
subPathExpr: ${POD_NAME}
```

---

## 10. 修改 `yarn-site.xml`

修改實際使用的 Hadoop 設定檔，例如：

```bash
vi /opt/zfs/sys/hadoop-3.5.0/etc/hadoop/yarn-site.xml
```

或如果使用原始模板：

```bash
vi ~/wulin/wk/dt/conf/hadoop-3.5.0/yarn-site.xml
```

將：

```xml
<property>
  <name>yarn.nodemanager.local-dirs</name>
  <value>/home/bigred/yarn</value>
</property>
```

改成：

```xml
<property>
  <name>yarn.nodemanager.local-dirs</name>
  <value>/home/bigred/yarn/local</value>
</property>

<property>
  <name>yarn.nodemanager.log-dirs</name>
  <value>/home/bigred/yarn/logs</value>
</property>
```

建議加入 cache/log 清理設定：

```xml
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

說明：

- `yarn.nodemanager.local-dirs`：NodeManager 用來放 application cache、localized resource、container working directory。
- `yarn.nodemanager.log-dirs`：NodeManager 存放 container log 的位置。
- `localizer.cache.cleanup.interval-ms`：多久檢查一次 localized cache。
- `localizer.cache.target-size-mb`：cache 清理後目標大小。
- `log.retain-seconds`：container log 保留秒數。

---

## 11. 啟動 YARN 並驗證

在 `dtadm`：

```bash
startyarn
```

確認 daemon：

```bash
hls
```

確認 YARN nodes：

```bash
yarn node -list -all
```

確認設定值：

```bash
hdfs getconf -confKey yarn.nodemanager.local-dirs
hdfs getconf -confKey yarn.nodemanager.log-dirs
```

預期：

```text
/home/bigred/yarn/local
/home/bigred/yarn/logs
```

---

## 12. 跑任務後觀察

跑小型 Spark / YARN 任務後，在 sea2 觀察：

```bash
du -sh \
  /opt/zfs/tkdt/yarn/dtw-0 \
  /opt/zfs/tkdt/yarn/dtw-1 \
  /opt/zfs/tkdt/yarn/dtw-2
```

確認 container root filesystem 沒有爆：

```bash
df -h /
```

確認 Kubernetes node 沒有 DiskPressure：

```bash
kubectl get nodes \
  -o custom-columns='NODE:.metadata.name,DISK:.status.conditions[?(@.type=="DiskPressure")].status'
```

預期 `DiskPressure` 為：

```text
False
```

---

## 13. 常見問題與排查

### 13.1 `/home/bigred/yarn/local` 或 `/home/bigred/yarn/logs` 不存在

處理：

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  kubectl exec -n dt "$pod" -- bash -lc 'mkdir -p /home/bigred/yarn/local /home/bigred/yarn/logs'
done
```

---

### 13.2 三台 dtw 都寫到同一個 owner

現象：

```text
dtw-0: dtw-2
dtw-1: dtw-2
dtw-2: dtw-2
```

檢查：

```bash
kubectl get statefulset dtw -n dt -o yaml | grep -A5 -B5 subPathExpr
```

如果看到：

```yaml
subPathExpr: ${POD_NAME}
```

改成：

```yaml
subPathExpr: $(POD_NAME)
```

然後重建 Pod：

```bash
kubectl apply -f ~/wulin/wk/dt/sts-dtw.yaml
kubectl delete pod -n dt dtw-0 dtw-1 dtw-2
kubectl get pod -n dt -w
```

---

### 13.3 sea2 找不到 `/opt/zfs/tkdt/yarn/dtw-X/owner`

先找實際 backing path：

```bash
sudo find /opt/zfs \
  -path '*/yarn/*/owner' \
  -o -path '*/yarn/owner' \
  2>/dev/null
```

再確認 Podman node 掛載：

```bash
sudo podman inspect tkdt-worker1 --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{println}}{{end}}'
sudo podman inspect tkdt-worker2 --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{println}}{{end}}'
sudo podman inspect tkdt-worker3 --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{println}}{{end}}'
```

要找這種對應：

```text
/opt/zfs/tkdt -> /opt/zfs
```

---

### 13.4 錯誤產生 `/opt/zfs/tkdt/yarn/${POD_NAME}`

確認內容：

```bash
sudo find '/opt/zfs/tkdt/yarn/${POD_NAME}' -maxdepth 3
```

如果確認只有測試的 owner/local/logs，且新的 `dtw-0/1/2` 目錄都正常，可以清掉：

```bash
sudo rm -rf '/opt/zfs/tkdt/yarn/${POD_NAME}'
```

注意：這只刪錯誤的 YARN 測試目錄，不是 HDFS。

---

## 14. 備案：如果 `subPathExpr` 仍不能正常展開

如果修成：

```yaml
subPathExpr: $(POD_NAME)
```

後仍然不能展開，可以改用 symlink 方案。

### 14.1 修改 `sts-dtw.yaml`

把：

```yaml
           - name: yarn-local
             mountPath: /home/bigred/yarn
             subPathExpr: $(POD_NAME)
```

改成：

```yaml
           - name: yarn-local
             mountPath: /opt/zfs/yarn
```

`env: POD_NAME` 可以移除。

### 14.2 套用並重建

```bash
kubectl apply -f ~/wulin/wk/dt/sts-dtw.yaml
kubectl delete pod -n dt dtw-0 dtw-1 dtw-2
kubectl get pod -n dt -w
```

### 14.3 在每個 dtw 建 symlink

```bash
for pod in dtw-0 dtw-1 dtw-2; do
  echo "===== $pod ====="
  kubectl exec -n dt "$pod" -- bash -lc '
    set -e
    h=$(hostname)

    mkdir -p /opt/zfs/yarn/$h/local /opt/zfs/yarn/$h/logs

    if [ -e /home/bigred/yarn ] && [ ! -L /home/bigred/yarn ]; then
      mv /home/bigred/yarn /home/bigred/yarn.bak.$(date +%Y%m%d%H%M%S)
    fi

    ln -sfn /opt/zfs/yarn/$h /home/bigred/yarn

    chmod -R 775 /opt/zfs/yarn/$h

    ls -ld /home/bigred/yarn
    readlink -f /home/bigred/yarn
    ls -ld /home/bigred/yarn/local /home/bigred/yarn/logs
  '
done
```

這樣每台 dtw 的：

```text
/home/bigred/yarn
```

會指向：

```text
/opt/zfs/yarn/dtw-0
/opt/zfs/yarn/dtw-1
/opt/zfs/yarn/dtw-2
```

`yarn-site.xml` 仍維持：

```xml
<property>
  <name>yarn.nodemanager.local-dirs</name>
  <value>/home/bigred/yarn/local</value>
</property>

<property>
  <name>yarn.nodemanager.log-dirs</name>
  <value>/home/bigred/yarn/logs</value>
</property>
```

---

## 15. 最終判斷標準

這個流程完成後，應該滿足：

- `dtw-0/1/2` 的 `/home/bigred/yarn` 都是 386G 左右的 ZFS-backed filesystem。
- `dtw-0/1/2` 各自寫入自己的 `/opt/zfs/tkdt/yarn/dtw-X`。
- `yarn.nodemanager.local-dirs` 指向 `/home/bigred/yarn/local`。
- `yarn.nodemanager.log-dirs` 指向 `/home/bigred/yarn/logs`。
- `/home/bigred/dn` 沒有被動到。
- `dn-dtw-0/1/2` PVC 沒有被刪。
- 沒有執行 `formathdfs`。

簡單說：

```text
YARN cache/log 與 HDFS DataNode storage 分離。
dtw Pod 重建後，YARN 暫存目錄仍能回到 ZFS-backed 位置。
HDFS 資料不受影響。
```
