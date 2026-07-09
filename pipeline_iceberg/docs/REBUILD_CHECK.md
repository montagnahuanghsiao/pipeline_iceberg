# 非正常關機後排查並清掉殘存任務

建議在 `dtadm` 裡執行，先查、確認，再刪。**不要一開始就清資料。**

## 0. 先確認基本服務狀態

```bash
# 意外中斷後 先等連線再 kci 登入
kci tkdt
```

## 登入叢集管理節點會清除所有異常 先跳過這步才能排查

```bash
# 登入腳本有內建清除異常 Pod
ssh tkdt -p 32200
#bigred@sea2:~$ ssh tkdt -p 32200
#bigred@tkdt's password:
#Welcome to Taroko K8S Console v25.12 (task,kubectl,mc,s3fs,regctl)

#delete all completed pods
#delete all errored pods
```

如果 HDFS / YARN 還沒啟動：

```bash
# 進入 dtadm後 確認基本服務
starthdfs
startyarn

hls
```

確認 DataNode：

```bash
hdfs dfsadmin -report
```

確認 YARN NodeManager：

```bash
yarn node -list
```

## 1. 查 YARN 正在殘留的任務

```bash
yarn application -list
```

也可以查所有狀態：

```bash
yarn application -list -appStates ALL
```

常見殘留狀態：`ACCEPTED`、`RUNNING`、`SUBMITTED`、`NEW`、`NEW_SAVING`

如果看到這些，代表 ResourceManager 還認為任務存在。

## 2. 查看單一 YARN application 詳細資訊

把 `<app_id>` 換成實際 ID，例如 `application_1710000000000_0001`。

```bash
yarn application -status <app_id>
```

看 log：

```bash
yarn logs -applicationId <app_id> | tail -n 100
```

如果 log 太多，可以先看 container 清單：

```bash
yarn logs -applicationId <app_id> -show_application_log_info
```

## 3. 刪除／終止 YARN 殘存任務

針對單一 application：

```bash
yarn application -kill <app_id>
```

如果有多個 RUNNING / ACCEPTED 任務，可以先列出：

```bash
yarn application -list -appStates RUNNING,ACCEPTED,SUBMITTED
```

逐一 kill：

```bash
yarn application -kill <app_id_1>
yarn application -kill <app_id_2>
yarn application -kill <app_id_3>
```

再確認：

```bash
yarn application -list
```

## 4. 查 NodeManager 上是否還有殘留 JVM

逐台查：

```bash
ssh dtw-0 jps -l
ssh dtw-1 jps -l
ssh dtw-2 jps -l
```

正常情況 worker 上應該主要只剩：`DataNode`、`NodeManager`、`Jps`

如果看到這些，通常是殘留任務：

- `YarnChild`
- `MRAppMaster`
- `ApplicationMaster`
- `CoarseGrainedExecutorBackend`
- `ExecutorLauncher`

## 5. 溫和刪除殘留 JVM

先查 PID：

```bash
ssh dtw-0 "jps -l"
ssh dtw-1 "jps -l"
ssh dtw-2 "jps -l"
```

用 `kill -15`，**不要一開始 `kill -9`**：

```bash
ssh dtw-0 "kill -15 <pid>"
ssh dtw-1 "kill -15 <pid>"
ssh dtw-2 "kill -15 <pid>"
```

等待幾秒後確認：

```bash
ssh dtw-0 "jps -l"
ssh dtw-1 "jps -l"
ssh dtw-2 "jps -l"
```

如果真的殺不掉，再用：

```bash
ssh dtw-0 "kill -9 <pid>"
```

## 6. 一次查出 worker 上疑似殘留任務 PID

```bash
for n in dtw-0 dtw-1 dtw-2
do
  echo "===== $n ====="
  ssh $n "jps -l | egrep 'YarnChild|MRAppMaster|ApplicationMaster|CoarseGrainedExecutor|ExecutorLauncher' || true"
done
```

刪除殘留任務，先 `kill -15`：

```bash
for n in dtw-0 dtw-1 dtw-2
do
  echo "===== $n ====="
  ssh $n "jps -l | egrep 'YarnChild|MRAppMaster|ApplicationMaster|CoarseGrainedExecutor|ExecutorLauncher' | awk '{print \$1}' | xargs -r kill -15"
done
```

再確認：

```bash
for n in dtw-0 dtw-1 dtw-2
do
  echo "===== $n ====="
  ssh $n "jps -l | egrep 'YarnChild|MRAppMaster|ApplicationMaster|CoarseGrainedExecutor|ExecutorLauncher' || true"
done
```

必要時才 `kill -9`：

```bash
for n in dtw-0 dtw-1 dtw-2
do
  echo "===== $n ====="
  ssh $n "jps -l | egrep 'YarnChild|MRAppMaster|ApplicationMaster|CoarseGrainedExecutor|ExecutorLauncher' | awk '{print \$1}' | xargs -r kill -9"
done
```

## 7. 查 HDFS 是否有未關閉檔案或 Lease 問題

```bash
hdfs fsck / -openforwrite
```

如果看到檔案，代表有檔案仍被視為寫入中。

查看完整 HDFS 健康狀態：

```bash
hdfs fsck / -files -blocks -locations | tail -n 50
```

查 under-replicated block：

```bash
hdfs fsck / | egrep 'UNDER MIN REPL|Under replicated|CORRUPT|MISSING'
```

## 8. 處理 HDFS safemode

查 safemode：

```bash
hdfs dfsadmin -safemode get
```

如果是 `ON`，先確認 DataNode 都回來：

```bash
hdfs dfsadmin -report | grep -i "Live datanodes"
```

應該是：`Live datanodes (3)`

如果三台都回來，但還卡 safemode，可以離開 safemode：

```bash
hdfs dfsadmin -safemode leave
```

再確認：

```bash
hdfs dfsadmin -safemode get
```

## 9. 清 YARN 暫存目錄

先查看，**不要直接刪**：

```bash
hdfs dfs -ls /tmp
hdfs dfs -ls /tmp/hadoop-yarn
hdfs dfs -ls /tmp/hadoop-yarn/staging
```

如果確認沒有任務在跑：

```bash
yarn application -list
```

沒有 RUNNING 後，可以清 staging：

```bash
hdfs dfs -rm -r -skipTrash /tmp/hadoop-yarn/staging/*
```

如果 Hive / Tez 有用 `/tmp/hive`，先查：

```bash
hdfs dfs -ls /tmp/hive
```

確認沒有正在跑的 Hive 查詢後再清：

```bash
hdfs dfs -rm -r -skipTrash /tmp/hive/*
```

Spark event log **不建議直接清**，除非你確定不要歷史紀錄：

```bash
hdfs dfs -ls /tmp/spark-events
```

要清再做：

```bash
hdfs dfs -rm -r -skipTrash /tmp/spark-events/*
```

## 10. 查 Kubernetes 裡是否有殘留 Pod

```bash
kubectl get pods -n dt -o wide
```

查所有非 Running / Completed：

```bash
kubectl get pods -A | grep -vE 'Running|Completed'
```

如果有 `Error`、`Evicted`、`ContainerStatusUnknown` 的 Pod，可以刪：

```bash
kubectl delete pod <pod-name> -n <namespace>
```

如果是 Job 產生的 Pod，先看 Job：

```bash
kubectl get jobs -A
```

刪指定 Job：

```bash
kubectl delete job <job-name> -n <namespace>
```

## 11. 清掉已失敗或成功的 Kubernetes Pod

先看：

```bash
kubectl get pods -A | egrep 'Completed|Error|Evicted|ContainerStatusUnknown'
```

刪 Completed：

```bash
kubectl delete pod -A --field-selector=status.phase=Succeeded
```

刪 Failed：

```bash
kubectl delete pod -A --field-selector=status.phase=Failed
```

## 12. 最後驗證

```bash
yarn application -list
```

應該沒有 RUNNING / ACCEPTED 殘留。

```bash
hdfs dfsadmin -safemode get
```

應該是：`Safe mode is OFF`

```bash
hdfs dfsadmin -report
```

應該看到：`Live datanodes (3)`

```bash
kubectl get pods -A | grep -vE 'Running|Completed'
```

理想狀態沒有異常 Pod。

```bash
dtest
```

最後用老師的測試腳本確認整體 Hadoop/YARN 可用。
