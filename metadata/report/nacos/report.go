/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nacos

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

import (
	gxset "github.com/dubbogo/gost/container/set"
	nacosClient "github.com/dubbogo/gost/database/kv/nacos"
	"github.com/dubbogo/gost/log/logger"

	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	perrors "github.com/pkg/errors"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/extension"
	"dubbo.apache.org/dubbo-go/v3/metadata/info"
	"dubbo.apache.org/dubbo-go/v3/metadata/mapping"
	"dubbo.apache.org/dubbo-go/v3/metadata/report"
	"dubbo.apache.org/dubbo-go/v3/registry"
	"dubbo.apache.org/dubbo-go/v3/remoting/nacos"
)

func init() {
	mf := &nacosMetadataReportFactory{}
	extension.SetMetadataReportFactory("nacos", func() report.MetadataReportFactory {
		return mf
	})
}

// nacosMetadataReport is the implementation
// of MetadataReport based on nacos.
type nacosMetadataReport struct {
	client    *nacosClient.NacosConfigClient
	group     string
	reportURL *common.URL // for Java per-interface fallback (HTTP to public namespace)
}

// GetAppMetadata get metadata info from nacos.
// Align with Java NacosMetadataReport.getAppMetadata: only (dataId=application, group=revision).
// try2 is for backward compat with dubbo-go 3.1.x which wrote (application+"::"+revision, reportGroup).
func (n *nacosMetadataReport) GetAppMetadata(application, revision string) (*info.MetadataInfo, error) {
	logger.Infof("[metadata-diag] Nacos GetAppMetadata: application=%q revision=%q reportGroup=%q", application, revision, n.group)
	var data string
	var err error
	// try1: same as Java — dataId=application, group=revision
	data, err = n.getConfig(vo.ConfigParam{DataId: application, Group: revision})
	if err != nil {
		logger.Warnf("[metadata-diag] Nacos GetAppMetadata try1 (dataId=%q group=%q) err=%v", application, revision, err)
	}
	if data == "" {
		logger.Infof("[metadata-diag] Nacos GetAppMetadata try2: dataId=%q group=%q (3.1.x compat)", application+constant.KeySeparator+revision, n.group)
		data, err = n.getConfig(vo.ConfigParam{
			DataId: application + constant.KeySeparator + revision,
			Group:  n.group,
		})
	}
	if data == "" {
		// Align with Java: when application-level not found, try Java per-interface metadata (dataId={interface}::{group}:provider:{application}, group=dubbo).
		meta, fallbackErr := n.getAppMetadataFromJavaPerInterface(application, revision)
		if fallbackErr != nil {
			logger.Warnf("[metadata-diag] Nacos GetAppMetadata: no data for application=%q revision=%q (all tries); Java per-interface fallback: %v", application, revision, fallbackErr)
			return nil, perrors.New("config data not exist")
		}
		logger.Infof("[metadata-diag] Nacos GetAppMetadata ok (Java per-interface fallback): application=%q revision=%q", application, revision)
		return meta, nil
	}
	var metadataInfo info.MetadataInfo
	if err = json.Unmarshal([]byte(data), &metadataInfo); err != nil {
		logger.Warnf("[metadata-diag] Nacos GetAppMetadata unmarshal err=%v", err)
		return nil, err
	}
	logger.Infof("[metadata-diag] Nacos GetAppMetadata ok: application=%q revision=%q", application, revision)
	return &metadataInfo, nil
}

// PublishAppMetadata publish metadata info to nacos.
// Align with Java: only (dataId=application, group=revision), same as NacosMetadataReport.publishAppMetadata.
func (n *nacosMetadataReport) PublishAppMetadata(application, revision string, meta *info.MetadataInfo) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return n.storeMetadata(vo.ConfigParam{
		DataId:  application,
		Group:   revision,
		Content: string(data),
	})
}

// storeMetadata will publish the metadata to Nacos
// if failed or error is not nil, error will be returned
func (n *nacosMetadataReport) storeMetadata(param vo.ConfigParam) error {
	res, err := n.client.Client().PublishConfig(param)
	if err != nil {
		return perrors.WithMessage(err, "Could not publish the metadata")
	}
	if !res {
		return perrors.New("Publish the metadata failed.")
	}
	return nil
}

// getConfig will read the config
func (n *nacosMetadataReport) getConfig(param vo.ConfigParam) (string, error) {
	logger.Infof("[metadata-diag] Nacos getConfig: dataId=%q group=%q", param.DataId, param.Group)
	cfg, err := n.client.Client().GetConfig(param)
	if err != nil {
		logger.Warnf("[metadata-diag] Nacos getConfig failed: dataId=%q group=%q err=%v", param.DataId, param.Group, err)
		return "", err
	}
	logger.Infof("[metadata-diag] Nacos getConfig ok: dataId=%q group=%q len=%d", param.DataId, param.Group, len(cfg))
	return cfg, nil
}

func (n *nacosMetadataReport) addListener(key string, group string, notify mapping.MappingListener) error {
	return n.client.Client().ListenConfig(vo.ConfigParam{
		DataId: key,
		Group:  group,
		OnChange: func(namespace, group, dataId, data string) {
			go callback(notify, dataId, data)
		},
	})
}

func callback(notify mapping.MappingListener, dataId, data string) {
	appNames := strings.Split(data, constant.CommaSeparator)
	set := gxset.NewSet()
	for _, app := range appNames {
		set.Add(app)
	}
	if err := notify.OnEvent(registry.NewServiceMappingChangedEvent(dataId, set)); err != nil {
		logger.Errorf("serviceMapping callback err: %s", err.Error())
	}
}

func (n *nacosMetadataReport) removeServiceMappingListener(key string, group string) error {
	return n.client.Client().CancelListenConfig(vo.ConfigParam{
		DataId: key,
		Group:  group,
	})
}

// RegisterServiceAppMapping map the specified Dubbo service interface to current Dubbo app name
func (n *nacosMetadataReport) RegisterServiceAppMapping(key string, group string, value string) error {
	oldVal, _ := n.getConfig(vo.ConfigParam{
		DataId: key,
		Group:  group,
	})
	if oldVal != "" {
		oldApps := strings.Split(oldVal, constant.CommaSeparator)
		if len(oldApps) > 0 {
			for _, app := range oldApps {
				if app == value {
					return nil
				}
			}
		}
		value = oldVal + constant.CommaSeparator + value
	}
	return n.storeMetadata(vo.ConfigParam{
		DataId:  key,
		Group:   group,
		Content: value,
	})
}

// GetServiceAppMapping get the app names from the specified Dubbo service interface
func (n *nacosMetadataReport) GetServiceAppMapping(key string, group string, listener mapping.MappingListener) (*gxset.HashSet, error) {
	// add service mapping listener
	if listener != nil {
		if err := n.addListener(key, group, listener); err != nil {
			logger.Errorf("add serviceMapping listener err: %s", err.Error())
		}
	}
	v, err := n.getConfig(vo.ConfigParam{
		DataId: key,
		Group:  group,
	})
	if err != nil {
		return nil, err
	}
	if v == "" {
		return nil, perrors.New("There is no service app mapping data.")
	}
	appNames := strings.Split(v, constant.CommaSeparator)
	set := gxset.NewSet()
	for _, e := range appNames {
		set.Add(e)
	}
	return set, nil
}

// RemoveServiceAppMappingListener remove the serviceMapping listener from metadata center
func (n *nacosMetadataReport) RemoveServiceAppMappingListener(key string, group string) error {
	return n.removeServiceMappingListener(key, group)
}

func (n *nacosMetadataReport) getAppMetadataFromJavaPerInterface(application, revision string) (*info.MetadataInfo, error) {
	if n.reportURL == nil || n.reportURL.Location == "" {
		return nil, perrors.New("report URL not set")
	}
	interfacesEnv := os.Getenv("DUBBO_JAVA_METADATA_INTERFACES")
	if interfacesEnv == "" {
		return nil, perrors.New("DUBBO_JAVA_METADATA_INTERFACES not set")
	}
	svcGroup := os.Getenv("DUBBO_METADATA_GROUP")
	if svcGroup == "" {
		svcGroup = "dubbo"
	}
	baseURL := "http://" + strings.TrimPrefix(strings.TrimPrefix(n.reportURL.Location, "nacos://"), "http://")
	namespacesToTry := []string{"public"}
	if ns := strings.TrimSpace(n.reportURL.GetParam(constant.NacosNamespaceID, "")); ns != "" && ns != "public" {
		namespacesToTry = append(namespacesToTry, ns)
	}
	configGroups := []string{"dubbo", n.group}
	client := &http.Client{Timeout: 5 * time.Second}
	meta := info.NewMetadataInfo(application, "")
	added := 0
	for _, iface := range strings.Split(interfacesEnv, ",") {
		iface = strings.TrimSpace(iface)
		if iface == "" {
			continue
		}
		dataID := iface + "::" + svcGroup + ":provider:" + application
		found := false
		for _, tenant := range namespacesToTry {
			if found {
				break
			}
			for _, cfgGroup := range configGroups {
				body, err := n.nacosConfigGetHTTP(baseURL, dataID, cfgGroup, tenant, client)
				if err != nil || body == "" || strings.Contains(body, "config data not exist") {
					continue
				}
				urls := parseExportedURLsFromJavaFullServiceDefinition(body)
				for _, u := range urls {
					if u == nil {
						continue
					}
					meta.AddService(u)
					added++
					logger.Infof("[metadata-diag] GetAppMetadata Java per-interface: dataId=%q group=%q tenant=%q -> URL %s", dataID, cfgGroup, tenant, u.String())
				}
				if len(urls) > 0 {
					found = true
					break
				}
			}
		}
	}
	if added == 0 {
		return nil, perrors.New("no Java per-interface metadata found in Nacos")
	}
	logger.Infof("[metadata-diag] GetAppMetadata Java per-interface: total %d services added to MetadataInfo", added)
	return meta, nil
}

func (n *nacosMetadataReport) nacosConfigGetHTTP(baseURL, dataId, group, tenant string, client *http.Client) (string, error) {
	reqURL := baseURL + "/nacos/v1/cs/configs?dataId=" + url.QueryEscape(dataId) + "&group=" + url.QueryEscape(group) + "&tenant=" + url.QueryEscape(tenant)
	resp, err := client.Get(reqURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		return "", err
	}
	s := strings.TrimSpace(string(body))
	if s == "" || strings.Contains(s, "config data not exist") {
		return "", perrors.New("config data not exist")
	}
	return s, nil
}

// parseExportedURLsFromJavaFullServiceDefinition extracts exported Dubbo URL(s) from Java FullServiceDefinition JSON.
// Java FullServiceDefinition has NO "url" field; instead it has "parameters" map with bind.ip, bind.port, interface, group, etc.
// We construct a Dubbo URL from parameters (same data Java uses internally).
func parseExportedURLsFromJavaFullServiceDefinition(body string) []*common.URL {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		logger.Warnf("[metadata-diag] parseFullServiceDefinition: JSON unmarshal failed: %v", err)
		return nil
	}
	// Try direct URL fields first (unlikely but safe)
	var urlStrs []string
	for _, key := range []string{"url", "exportedServiceURL", "serviceUrl"} {
		if v, ok := m[key].(string); ok && v != "" {
			urlStrs = append(urlStrs, v)
		}
	}
	if arr, ok := m["urls"].([]interface{}); ok {
		for _, u := range arr {
			if s, ok := u.(string); ok && s != "" {
				urlStrs = append(urlStrs, s)
			}
		}
	}
	var out []*common.URL
	for _, s := range urlStrs {
		u, err := common.NewURL(s)
		if err != nil {
			continue
		}
		out = append(out, u)
	}
	if len(out) > 0 {
		return out
	}

	// Java FullServiceDefinition: build URL from parameters map
	params, ok := m["parameters"]
	if !ok {
		logger.Warnf("[metadata-diag] parseFullServiceDefinition: no 'parameters' field")
		return nil
	}
	paramsMap, ok := params.(map[string]interface{})
	if !ok {
		return nil
	}
	str := func(key string) string {
		if v, ok := paramsMap[key]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
		return ""
	}
	ip := str("bind.ip")
	port := str("bind.port")
	iface := str("interface")
	if ip == "" || port == "" || iface == "" {
		logger.Warnf("[metadata-diag] parseFullServiceDefinition: missing bind.ip=%q bind.port=%q interface=%q", ip, port, iface)
		return nil
	}
	// Determine protocol: Java uses tri/triple for Triple protocol; default to tri
	protocol := "tri"
	if p := str("protocol"); p != "" {
		protocol = p
	}
	// Build query string from all parameters (align with Java: invoker URL carries all params including token)
	q := url.Values{}
	for k, v := range paramsMap {
		if s, ok := v.(string); ok {
			q.Set(k, s)
		}
	}
	// Ensure interface is set in params
	q.Set(constant.InterfaceKey, iface)
	rawURL := protocol + "://" + ip + ":" + port + "/" + iface + "?" + q.Encode()
	u, err := common.NewURL(rawURL)
	if err != nil {
		logger.Warnf("[metadata-diag] parseFullServiceDefinition: NewURL failed: %v (raw=%s)", err, rawURL[:min(len(rawURL), 200)])
		return nil
	}
	logger.Infof("[metadata-diag] parseFullServiceDefinition: built URL from parameters: %s:%s protocol=%s interface=%s token_len=%d",
		ip, port, protocol, iface, len(str("token")))
	return []*common.URL{u}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type nacosMetadataReportFactory struct{}

// CreateMetadataReport creates the nacos-based metadata report implementation.
func (n *nacosMetadataReportFactory) CreateMetadataReport(url *common.URL) report.MetadataReport {
	namespace := url.GetParam(constant.MetadataReportNamespaceKey, "")
	url.SetParam(constant.NacosNamespaceID, namespace)
	url.SetParam(constant.TimeoutKey, url.GetParam(constant.TimeoutKey, constant.DefaultRegTimeout))
	group := url.GetParam(constant.MetadataReportGroupKey, constant.ServiceDiscoveryDefaultGroup)
	url.SetParam(constant.NacosGroupKey, group)
	url.SetParam(constant.NacosUsername, url.Username)
	url.SetParam(constant.NacosPassword, url.Password)
	logger.Infof("[Nacos MetadataReport] created with namespace=%q group=%q (align Java: use public when registry as metadata center)", namespace, group)
	client, err := nacos.NewNacosConfigClientByUrl(url)
	if err != nil {
		logger.Errorf("Could not create nacos metadata report. URL: %s", url.String())
		return nil
	}
	return &nacosMetadataReport{client: client, group: group, reportURL: url.Clone()}
}
