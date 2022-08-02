// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

import (
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"go.uber.org/zap"
)

var pulsarOnce sync.Once

const (
	// SuggestPulsarMaxMessageSize defines the maximum size of Pulsar message.
	SuggestPulsarMaxMessageSize = 5 * 1024 * 1024
	defaultEtcdLogLevel         = "info"
	defaultEtcdLogPath          = "stdout"
)

// ServiceParam is used to quickly and easily access all basic service configurations.
type ServiceParam struct {
	BaseTable

	LocalStorageCfg LocalStorageConfig
	MetaStoreCfg    MetaStoreConfig
	EtcdCfg         EtcdConfig
	DBCfg           MetaDBConfig
	PulsarCfg       PulsarConfig
	KafkaCfg        KafkaConfig
	RocksmqCfg      RocksmqConfig
	MinioCfg        MinioConfig
}

func (p *ServiceParam) Init() {
	p.BaseTable.Init()

	p.LocalStorageCfg.init(&p.BaseTable)
	p.MetaStoreCfg.init(&p.BaseTable)
	p.EtcdCfg.init(&p.BaseTable)
	if p.MetaStoreCfg.MetaStoreType == util.MetaStoreTypeMysql {
		log.Debug("Mysql protocol is used as meta store")
		p.DBCfg.init(&p.BaseTable)
	}
	p.PulsarCfg.init(&p.BaseTable)
	p.KafkaCfg.init(&p.BaseTable)
	p.RocksmqCfg.init(&p.BaseTable)
	p.MinioCfg.init(&p.BaseTable)
}

///////////////////////////////////////////////////////////////////////////////
// --- etcd ---
type EtcdConfig struct {
	Base *BaseTable

	// --- ETCD ---
	Endpoints         []string
	MetaRootPath      string
	KvRootPath        string
	EtcdLogLevel      string
	EtcdLogPath       string
	EtcdUseSSL        bool
	EtcdTLSCert       string
	EtcdTLSKey        string
	EtcdTLSCACert     string
	EtcdTLSMinVersion string

	// --- Embed ETCD ---
	UseEmbedEtcd bool
	ConfigPath   string
	DataDir      string
}

func (p *EtcdConfig) init(base *BaseTable) {
	p.Base = base
	p.LoadCfgToMemory()
}

func (p *EtcdConfig) LoadCfgToMemory() {
	p.initUseEmbedEtcd()
	if p.UseEmbedEtcd {
		p.initConfigPath()
		p.initDataDir()
	} else {
		p.initEndpoints()
	}
	p.initMetaRootPath()
	p.initKvRootPath()
	p.initEtcdLogLevel()
	p.initEtcdLogPath()
	p.initEtcdUseSSL()
	p.initEtcdTLSCert()
	p.initEtcdTLSKey()
	p.initEtcdTLSCACert()
	p.initEtcdTLSMinVersion()
}

func (p *EtcdConfig) initUseEmbedEtcd() {
	p.UseEmbedEtcd = p.Base.ParseBool("etcd.use.embed", false)
	if p.UseEmbedEtcd && (os.Getenv(metricsinfo.DeployModeEnvKey) != metricsinfo.StandaloneDeployMode) {
		panic("embedded etcd can not be used under distributed mode")
	}
}

func (p *EtcdConfig) initConfigPath() {
	addr := p.Base.LoadWithDefault("etcd.config.path", "")
	p.ConfigPath = addr
}

func (p *EtcdConfig) initDataDir() {
	addr := p.Base.LoadWithDefault("etcd.data.dir", "default.etcd")
	p.DataDir = addr
}

func (p *EtcdConfig) initEndpoints() {
	endpoints, err := p.Base.Load("etcd.endpoints")
	if err != nil {
		panic(err)
	}
	p.Endpoints = strings.Split(endpoints, ",")
}

func (p *EtcdConfig) initMetaRootPath() {
	rootPath, err := p.Base.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Base.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = path.Join(rootPath, subPath)
}

func (p *EtcdConfig) initKvRootPath() {
	rootPath, err := p.Base.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Base.Load("etcd.kvSubPath")
	if err != nil {
		panic(err)
	}
	p.KvRootPath = path.Join(rootPath, subPath)
}

func (p *EtcdConfig) initEtcdLogLevel() {
	p.EtcdLogLevel = p.Base.LoadWithDefault("etcd.log.level", defaultEtcdLogLevel)
}

func (p *EtcdConfig) initEtcdLogPath() {
	p.EtcdLogPath = p.Base.LoadWithDefault("etcd.log.path", defaultEtcdLogPath)
}

func (p *EtcdConfig) initEtcdUseSSL() {
	p.EtcdUseSSL = p.Base.ParseBool("etcd.ssl.enabled", false)
}

func (p *EtcdConfig) initEtcdTLSCert() {
	p.EtcdTLSCert = p.Base.LoadWithDefault("etcd.ssl.tlsCert", "")
}

func (p *EtcdConfig) initEtcdTLSKey() {
	p.EtcdTLSKey = p.Base.LoadWithDefault("etcd.ssl.tlsKey", "")
}

func (p *EtcdConfig) initEtcdTLSCACert() {
	p.EtcdTLSCACert = p.Base.LoadWithDefault("etcd.ssl.tlsCACert", "")
}

func (p *EtcdConfig) initEtcdTLSMinVersion() {
	p.EtcdTLSMinVersion = p.Base.LoadWithDefault("etcd.ssl.tlsMinVersion", "1.3")
}

type LocalStorageConfig struct {
	Path ParamItem
}

func (p *LocalStorageConfig) init(base *BaseTable) {
	p.Path = ParamItem{
		Key:          "localStorage.path",
		Version:      "2.0.0",
		DefaultValue: "/var/lib/milvus/data",
		Refreshable:  false,
	}
	p.Path.Init(base.mgr)
}

type MetaStoreConfig struct {
	Base *BaseTable

	MetaStoreType string
}

func (p *MetaStoreConfig) init(base *BaseTable) {
	p.Base = base
	p.initMetaStoreType()
}

func (p *MetaStoreConfig) initMetaStoreType() {
	p.MetaStoreType = p.Base.LoadWithDefault("metastore.type", util.MetaStoreTypeEtcd)
}

///////////////////////////////////////////////////////////////////////////////
// --- meta db ---
type MetaDBConfig struct {
	Base *BaseTable

	Username     string
	Password     string
	Address      string
	Port         int
	DBName       string
	MaxOpenConns int
	MaxIdleConns int
}

func (p *MetaDBConfig) init(base *BaseTable) {
	p.Base = base
	p.LoadCfgToMemory()
}

func (p *MetaDBConfig) LoadCfgToMemory() {
	p.initUsername()
	p.initPassword()
	p.initAddress()
	p.initPort()
	p.initDbName()
	p.initMaxOpenConns()
	p.initMaxIdleConns()
}

func (p *MetaDBConfig) initUsername() {
	username, err := p.Base.Load("mysql.username")
	if err != nil {
		panic(err)
	}
	p.Username = username
}

func (p *MetaDBConfig) initPassword() {
	password, err := p.Base.Load("mysql.password")
	if err != nil {
		panic(err)
	}
	p.Password = password
}

func (p *MetaDBConfig) initAddress() {
	address, err := p.Base.Load("mysql.address")
	if err != nil {
		panic(err)
	}
	p.Address = address
}

func (p *MetaDBConfig) initPort() {
	port := p.Base.ParseIntWithDefault("mysql.port", 3306)
	p.Port = port
}

func (p *MetaDBConfig) initDbName() {
	dbName, err := p.Base.Load("mysql.dbName")
	if err != nil {
		panic(err)
	}
	p.DBName = dbName
}

func (p *MetaDBConfig) initMaxOpenConns() {
	maxOpenConns := p.Base.ParseIntWithDefault("mysql.maxOpenConns", 20)
	p.MaxOpenConns = maxOpenConns
}

func (p *MetaDBConfig) initMaxIdleConns() {
	maxIdleConns := p.Base.ParseIntWithDefault("mysql.maxIdleConns", 5)
	p.MaxIdleConns = maxIdleConns
}

///////////////////////////////////////////////////////////////////////////////
// --- pulsar ---
type PulsarConfig struct {
	Address        ParamItem
	Port           ParamItem
	WebAddress     ParamItem
	WebPort        ParamItem
	MaxMessageSize ParamItem
}

func (p *PulsarConfig) init(base *BaseTable) {
	p.Port = ParamItem{
		Key:          "pulsar.port",
		Version:      "2.0.0",
		DefaultValue: "6650",
		Refreshable:  false,
	}
	p.Port.Init(base.mgr)

	p.Address = ParamItem{
		Key:          "pulsar.address",
		Version:      "2.0.0",
		DefaultValue: "localhost",
		Refreshable:  false,
		GetFunc: func(addr string) string {
			if addr == "" {
				return ""
			}
			if strings.Contains(addr, ":") {
				return addr
			}
			port, _ := p.Port.GetValue()
			return "pulsar://" + addr + ":" + port
		},
	}
	p.Address.Init(base.mgr)

	p.WebPort = ParamItem{
		Key:          "pulsar.webport",
		Version:      "2.0.0",
		DefaultValue: "80",
		Refreshable:  true,
	}
	p.WebPort.Init(base.mgr)

	p.WebAddress = ParamItem{
		Key:          "pulsar.webaddress",
		Version:      "2.0.0",
		DefaultValue: "",
		Refreshable:  false,
		GetFunc: func(add string) string {
			pulsarURL, err := url.ParseRequestURI(p.Address.GetAsString())
			if err != nil {
				log.Info("failed to parse pulsar config, assume pulsar not used", zap.Error(err))
				return ""
			}
			return "http://" + pulsarURL.Hostname() + ":" + p.WebPort.GetAsString()
		},
	}
	p.WebAddress.Init(base.mgr)

	p.MaxMessageSize = ParamItem{
		Key:          "pulsar.maxMessageSize",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(SuggestPulsarMaxMessageSize),
		Refreshable:  true,
	}
	p.MaxMessageSize.Init(base.mgr)

}

// --- kafka ---
type KafkaConfig struct {
	Address          ParamItem
	SaslUsername     ParamItem
	SaslPassword     ParamItem
	SaslMechanisms   ParamItem
	SecurityProtocol ParamItem
}

func (k *KafkaConfig) init(base *BaseTable) {
	k.Address = ParamItem{
		Key:          "kafka.brokerList",
		DefaultValue: "",
		Version:      "2.1.0",
		Refreshable:  false,
	}
	k.Address.Init(base.mgr)

	k.SaslUsername = ParamItem{
		Key:          "kafka.saslUsername",
		DefaultValue: "",
		Version:      "2.1.0",
		Refreshable:  false,
	}
	k.SaslUsername.Init(base.mgr)

	k.SaslPassword = ParamItem{
		Key:          "kafka.saslPassword",
		DefaultValue: "",
		Version:      "2.1.0",
		Refreshable:  false,
	}
	k.SaslPassword.Init(base.mgr)

	k.SaslMechanisms = ParamItem{
		Key:          "kafka.saslMechanisms",
		DefaultValue: "PLAIN",
		Version:      "2.1.0",
		Refreshable:  false,
	}
	k.SaslMechanisms.Init(base.mgr)

	k.SecurityProtocol = ParamItem{
		Key:          "kafka.securityProtocol",
		DefaultValue: "SASL_SSL",
		Version:      "2.1.0",
		Refreshable:  false,
	}
	k.SecurityProtocol.Init(base.mgr)
}

///////////////////////////////////////////////////////////////////////////////
// --- rocksmq ---
type RocksmqConfig struct {
	Path ParamItem
}

func (r *RocksmqConfig) init(base *BaseTable) {
	r.Path = ParamItem{
		Key:          "rocksmq.path",
		DefaultValue: "",
		Version:      "2.0.0",
		Refreshable:  false,
	}
	r.Path.Init(base.mgr)
}

///////////////////////////////////////////////////////////////////////////////
// --- minio ---
type MinioConfig struct {
	Address         ParamItem
	Port            ParamItem
	AccessKeyID     ParamItem
	SecretAccessKey ParamItem
	UseSSL          ParamItem
	BucketName      ParamItem
	RootPath        ParamItem
	UseIAM          ParamItem
	IAMEndpoint     ParamItem
}

func (p *MinioConfig) init(base *BaseTable) {
	p.Port = ParamItem{
		Key:          "minio.port",
		DefaultValue: "9000",
		Version:      "2.0.0",
		Refreshable:  false,
	}
	p.Port.Init(base.mgr)

	p.Address = ParamItem{
		Key:          "minio.address",
		DefaultValue: "",
		Version:      "2.0.0",
		GetFunc: func(addr string) string {
			if addr == "" {
				return ""
			}
			if strings.Contains(addr, ":") {
				return addr
			}
			port, _ := p.Port.GetValue()
			return addr + ":" + port
		},
		Refreshable: false,
	}
	p.Address.Init(base.mgr)

	p.AccessKeyID = ParamItem{
		Key:          "minio.accessKeyID",
		Version:      "2.0.0",
		Refreshable:  false,
		PanicIfEmpty: true,
	}
	p.AccessKeyID.Init(base.mgr)

	p.SecretAccessKey = ParamItem{
		Key:          "minio.secretAccessKey",
		Version:      "2.0.0",
		Refreshable:  false,
		PanicIfEmpty: true,
	}
	p.SecretAccessKey.Init(base.mgr)

	p.UseSSL = ParamItem{
		Key:          "minio.useSSL",
		Version:      "2.0.0",
		Refreshable:  false,
		PanicIfEmpty: true,
	}
	p.UseSSL.Init(base.mgr)

	p.BucketName = ParamItem{
		Key:          "minio.bucketName",
		Version:      "2.0.0",
		Refreshable:  false,
		PanicIfEmpty: true,
	}
	p.BucketName.Init(base.mgr)

	p.RootPath = ParamItem{
		Key:          "minio.rootPath",
		Version:      "2.0.0",
		Refreshable:  false,
		PanicIfEmpty: true,
	}
	p.RootPath.Init(base.mgr)

	p.UseIAM = ParamItem{
		Key:          "minio.useIAM",
		DefaultValue: DefaultMinioUseIAM,
		Version:      "2.0.0",
		Refreshable:  false,
	}
	p.UseIAM.Init(base.mgr)

	p.IAMEndpoint = ParamItem{
		Key:          "minio.iamEndpoint",
		DefaultValue: DefaultMinioIAMEndpoint,
		Version:      "2.0.0",
		Refreshable:  false,
	}
	p.IAMEndpoint.Init(base.mgr)
}
