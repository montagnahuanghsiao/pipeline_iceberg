# 前端最小打包與跨電腦搬移流程

本文件只打包前端建置與部署所需內容，不包含 Bronze、Silver、Gold、Serving Parquet、Spark Job、HDFS 資料或 Flask API 程式。

## 1. 打包內容

打包檔案包含：

- `frontend/`：HTML、CSS、JavaScript、地圖元件與前端資源。
- `pipeline_iceberg/deploy/docker/Dockerfile.frontend`：前端映像建置規則。
- `pipeline_iceberg/deploy/nginx/`：Nginx 設定與 `runtime-config.js`。
- `pipeline_iceberg/deploy/kubernetes/08-frontend.yaml`：Kubernetes 前端 Deployment 與 Service。

這些路徑會保留原本的專案結構，因此目標電腦解壓縮後，可以直接從專案根目錄建置映像。

## 2. 在來源 Linux 虛擬機打包

進入專案根目錄：

```bash
cd /opt/zfs/project
```

執行以下指令，建立壓縮檔與 SHA-256 驗證檔：

```bash
BUNDLE="ocean-frontend-bundle-$(date -u +%Y%m%dT%H%M%SZ).tar.gz"

tar -czf "$BUNDLE" \
  frontend \
  pipeline_iceberg/deploy/docker/Dockerfile.frontend \
  pipeline_iceberg/deploy/nginx \
  pipeline_iceberg/deploy/kubernetes/08-frontend.yaml

sha256sum "$BUNDLE" > "${BUNDLE}.sha256"

echo "BUNDLE=$BUNDLE"
ls -lh "$BUNDLE" "${BUNDLE}.sha256"
```

如果只需要一行版本：

```bash
cd /opt/zfs/project && BUNDLE="ocean-frontend-bundle-$(date -u +%Y%m%dT%H%M%SZ).tar.gz" && tar -czf "$BUNDLE" frontend pipeline_iceberg/deploy/docker/Dockerfile.frontend pipeline_iceberg/deploy/nginx pipeline_iceberg/deploy/kubernetes/08-frontend.yaml && sha256sum "$BUNDLE" > "${BUNDLE}.sha256" && echo "$BUNDLE"
```

## 3. 打包後先檢查內容

查看壓縮檔內容：

```bash
tar -tzf "$BUNDLE" | less
```

確認必要檔案存在：

```bash
for file in \
  frontend/index.html \
  pipeline_iceberg/deploy/docker/Dockerfile.frontend \
  pipeline_iceberg/deploy/nginx/nginx.conf \
  pipeline_iceberg/deploy/nginx/runtime-config.js \
  pipeline_iceberg/deploy/kubernetes/08-frontend.yaml; do
  tar -tzf "$BUNDLE" | grep -qx "$file" \
    && echo "OK  $file" \
    || echo "缺少 $file"
done
```

## 4. 搬移到另一臺電腦

### 方法 A：使用 SCP

將 `TARGET_USER`、`TARGET_IP` 和目標路徑換成實際值：

```bash
scp "$BUNDLE" "${BUNDLE}.sha256" \
  TARGET_USER@TARGET_IP:/home/TARGET_USER/
```

如果 SSH 使用自訂連接埠，例如 `22101`：

```bash
scp -P 22101 "$BUNDLE" "${BUNDLE}.sha256" \
  TARGET_USER@TARGET_IP:/home/TARGET_USER/
```

### 方法 B：使用共用資料夾或隨身碟

必須同時搬移以下兩個檔案：

```text
ocean-frontend-bundle-時間戳記.tar.gz
ocean-frontend-bundle-時間戳記.tar.gz.sha256
```

## 5. 在目標 Linux 電腦驗證並解壓縮

建立工作目錄：

```bash
mkdir -p ~/ocean-frontend-project
cd ~/ocean-frontend-project
```

將收到的兩個檔案放入此目錄，接著執行：

```bash
sha256sum -c ocean-frontend-bundle-*.tar.gz.sha256
```

結果必須顯示：

```text
ocean-frontend-bundle-....tar.gz: OK
```

再解壓縮：

```bash
tar -xzf ocean-frontend-bundle-*.tar.gz
```

確認目錄結構：

```bash
test -f frontend/index.html && echo "frontend OK"
test -f pipeline_iceberg/deploy/docker/Dockerfile.frontend && echo "Dockerfile OK"
test -f pipeline_iceberg/deploy/nginx/nginx.conf && echo "Nginx OK"
```

## 6. 先決定 API 連線方式

前端本身可以顯示頁面，但地圖、Dashboard 與指標資料仍需要 Flask API。

### 情境 A：部署到原本的 Kubernetes 叢集

保留以下設定即可：

```javascript
window.OCEAN_CONFIG = {
  dataSource: "api",
  apiBaseUrl: "/api/v1",
};
```

Nginx 會把 `/api/` 請求轉送至：

```text
http://ocean-flask-api:8000/api/
```

必要條件：

- `ocean-frontend` 與 `ocean-flask-api` 位於同一個 Kubernetes namespace。
- namespace 內存在名為 `ocean-flask-api` 的 Service。
- API Pod 必須為 Ready。

驗證：

```bash
kubectl get pod,svc -n dt -l app=ocean-flask-api
```

### 情境 B：前端搬到另一臺獨立電腦，API 留在原叢集

編輯：

```text
pipeline_iceberg/deploy/nginx/runtime-config.js
```

將 API 位址改成瀏覽器可以直接連線的實際位址，例如：

```javascript
window.OCEAN_CONFIG = {
  dataSource: "api",
  apiBaseUrl: "http://172.22.136.3:30800/api/v1",
};
```

注意：

- 這個位址是「使用者的瀏覽器」需要能連線的位址，不只是前端容器能連線。
- API 必須允許前端來源的 CORS 請求。
- 如果前端使用 HTTPS，API 也應使用 HTTPS，否則瀏覽器可能封鎖混合內容。

## 7. 使用 Podman 建置並啟動

在解壓縮後的專案根目錄執行：

```bash
cd ~/ocean-frontend-project

TAG="transfer-$(date -u +%Y%m%dT%H%M%SZ)"
IMAGE="ocean-frontend:${TAG}"

sudo podman build \
  -t "$IMAGE" \
  -f pipeline_iceberg/deploy/docker/Dockerfile.frontend \
  .

echo "IMAGE=$IMAGE"
```

不需要為了版本號修改 Dockerfile；版本放在映像標籤即可。

啟動容器：

```bash
sudo podman run -d \
  --name ocean-frontend \
  --restart=unless-stopped \
  -p 8080:8080 \
  "$IMAGE"
```

驗證首頁與執行階段設定：

```bash
curl -I http://127.0.0.1:8080/
curl -sS http://127.0.0.1:8080/runtime-config.js
```

瀏覽器開啟：

```text
http://目標電腦IP:8080/
```

若目標電腦有防火牆，需允許 TCP 8080。Ubuntu 使用 UFW 時：

```bash
sudo ufw allow 8080/tcp
```

## 8. 部署到 Kubernetes

### 8.1 建置並推送到目標 Registry

```bash
REGISTRY="dkreg.taroko:5000"
TAG="0.5.3"
IMAGE="${REGISTRY}/ocean-frontend:${TAG}"

sudo podman build \
  -t "$IMAGE" \
  -f pipeline_iceberg/deploy/docker/Dockerfile.frontend \
  .

sudo podman push \
  --tls-verify=false \
  --creds 'REGISTRY_USER:REGISTRY_PASSWORD' \
  "$IMAGE"
```

不要把真實帳號或密碼寫進文件、Shell Script 或 Git。

### 8.2 更新 Deployment 使用的映像

可以編輯 `08-frontend.yaml` 的 `image:`，或套用 YAML 後使用：

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/08-frontend.yaml

kubectl set image -n dt \
  deployment/ocean-frontend \
  frontend="$IMAGE"
```

等待部署完成：

```bash
kubectl rollout status -n dt \
  deployment/ocean-frontend \
  --timeout=5m
```

檢查實際使用的映像：

```bash
kubectl get deployment ocean-frontend -n dt \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

檢查 Pod 與 Service：

```bash
kubectl get pod,svc -n dt -l app=ocean-frontend -o wide
```

目前 YAML 使用 NodePort `30801`，可從瀏覽器開啟：

```text
http://任一可連線的Kubernetes節點IP:30801/
```

## 9. 完整驗收

### 9.1 靜態前端驗收

```bash
curl -I http://目標位址/
curl -sS http://目標位址/runtime-config.js
```

首頁應回傳 `HTTP 200`，而 `runtime-config.js` 應顯示預期的 API 位址。

### 9.2 API 驗收

若使用 Kubernetes 內部代理：

```bash
kubectl run ocean-frontend-curl -n dt \
  --rm -it --restart=Never \
  --image=curlimages/curl:8.12.1 -- \
  curl -i --max-time 10 \
  http://ocean-flask-api:8000/healthz
```

若 API 使用 NodePort：

```bash
curl -i --max-time 10 \
  http://API節點IP:30800/healthz
```

### 9.3 瀏覽器驗收

1. 使用 `Ctrl + F5` 強制重新載入，避免舊版 CSS 或 JavaScript 快取。
2. 開啟瀏覽器開發者工具的 Network 頁籤。
3. 確認 `index.html`、CSS、JavaScript 與 `runtime-config.js` 都回傳 `200`。
4. 確認 `/api/v1/...` 請求回傳 `200`，沒有 DNS、CORS、`404` 或 `502`。
5. 切換臺灣周邊與西北太平洋，確認地圖網格、經緯線、圖例與 Dashboard 均能顯示。

## 10. 常見問題

### 頁面能開，但地圖與圖表沒有資料

代表靜態前端正常，但 API 不可達、API 位址錯誤、CORS 被封鎖，或 Serving 資料不存在。先查看瀏覽器 Network 與 Console。

### 出現 `502 Bad Gateway`

Nginx 無法連到 `ocean-flask-api:8000`。確認 Service 名稱、namespace、API Pod Ready 狀態與 Nginx 設定。

### 更新後仍看到舊畫面

依序確認：

```bash
sudo podman images | grep ocean-frontend
kubectl get deployment ocean-frontend -n dt \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

再對 Deployment 重新啟動：

```bash
kubectl rollout restart -n dt deployment/ocean-frontend
kubectl rollout status -n dt deployment/ocean-frontend --timeout=5m
```

最後在瀏覽器按 `Ctrl + F5`。

### 只有前端壓縮檔，能否在完全離線的電腦顯示完整資料？

不能。這個壓縮檔只包含前端程式：

- 可以顯示 HTML、CSS、版面與靜態資源。
- 動態地圖資料、Dashboard 指標和日期查詢仍依賴 Flask API 與 Serving 資料。

如果目標電腦完全無法連到原本 API，還需要另外搬移 API 映像、Serving 資料及相關部署設定；那會是另一份「完整展示環境」搬移包，不屬於本文件的前端最小包。

## 11. 清理測試容器與舊映像

停止並刪除獨立 Podman 容器：

```bash
sudo podman stop ocean-frontend
sudo podman rm ocean-frontend
```

確認映像後再刪除指定舊標籤：

```bash
sudo podman images | grep ocean-frontend
sudo podman rmi ocean-frontend:舊標籤
```

不要直接執行無條件的全系統清理，以免刪除仍被 Kubernetes 節點或其他專案使用的映像。
