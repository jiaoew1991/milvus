// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package paramtable

import (
	"os"
	"testing"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/stretchr/testify/assert"
)

func TestServiceParam(t *testing.T) {
	var SParams ServiceParam
	SParams.Init()

	t.Run("test etcdConfig", func(t *testing.T) {
		Params := SParams.EtcdCfg

		assert.NotZero(t, len(Params.Endpoints))
		t.Logf("etcd endpoints = %s", Params.Endpoints)

		assert.NotEqual(t, Params.MetaRootPath, "")
		t.Logf("meta root path = %s", Params.MetaRootPath)

		assert.NotEqual(t, Params.KvRootPath, "")
		t.Logf("kv root path = %s", Params.KvRootPath)

		assert.NotNil(t, Params.EtcdUseSSL)
		t.Logf("use ssl = %t", Params.EtcdUseSSL)

		assert.NotEmpty(t, Params.EtcdTLSKey)
		t.Logf("tls key = %s", Params.EtcdTLSKey)

		assert.NotEmpty(t, Params.EtcdTLSCACert)
		t.Logf("tls CACert = %s", Params.EtcdTLSCACert)

		assert.NotEmpty(t, Params.EtcdTLSCert)
		t.Logf("tls cert = %s", Params.EtcdTLSCert)

		assert.NotEmpty(t, Params.EtcdTLSMinVersion)
		t.Logf("tls minVersion = %s", Params.EtcdTLSMinVersion)

		// test UseEmbedEtcd
		Params.Base.Save("etcd.use.embed", "true")
		assert.Nil(t, os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode))
		assert.Panics(t, func() { Params.initUseEmbedEtcd() })

		assert.Nil(t, os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode))
		Params.LoadCfgToMemory()
	})

	t.Run("test pulsarConfig", func(t *testing.T) {
		{
			assert.NotEqual(t, SParams.PulsarCfg.Address.GetAsString(), "")
			t.Logf("pulsar address = %s", SParams.PulsarCfg.Address.GetAsString())
			assert.Equal(t, SParams.PulsarCfg.MaxMessageSize.GetAsInt(), SuggestPulsarMaxMessageSize)
		}

		address := "pulsar://localhost:6650"
		{
			SParams.BaseTable.Save("pulsar.address", address)
			assert.Equal(t, SParams.PulsarCfg.Address.GetAsString(), address)
		}

		{
			SParams.BaseTable.Save("pulsar.address", "localhost")
			SParams.BaseTable.Save("pulsar.port", "6650")
			assert.Equal(t, SParams.PulsarCfg.Address.GetAsString(), address)
		}
	})

	t.Run("test pulsar web config", func(t *testing.T) {
		assert.NotEqual(t, SParams.PulsarCfg.Address.GetAsString(), "")

		{
			assert.NotEqual(t, SParams.PulsarCfg.WebAddress.GetAsString(), "")
		}

		{
			SParams.PulsarCfg.Address.updateValue(SParams.PulsarCfg.Address.GetAsString()+"invalid", false)
			assert.Equal(t, SParams.PulsarCfg.WebAddress.GetAsString(), "")
		}

		{
			SParams.PulsarCfg.Address.updateValue("", false)
			assert.Equal(t, SParams.PulsarCfg.WebAddress.GetAsString(), "")
		}
	})

	t.Run("test rocksmqConfig", func(t *testing.T) {
		Params := &SParams.RocksmqCfg

		assert.NotEqual(t, Params.Path.GetAsString(), "")
		t.Logf("rocksmq path = %s", Params.Path.GetAsString())
	})

	t.Run("test minioConfig", func(t *testing.T) {
		Params := &SParams.MinioCfg

		addr := Params.Address.GetAsString()
		equal := addr == "localhost:9000" || addr == "minio:9000"
		assert.Equal(t, equal, true)
		t.Logf("minio address = %s", Params.Address.GetAsString())

		assert.Equal(t, Params.AccessKeyID.GetAsString(), "minioadmin")

		assert.Equal(t, Params.SecretAccessKey.GetAsString(), "minioadmin")

		assert.Equal(t, Params.UseSSL.GetAsBool(), false)

		assert.Equal(t, Params.UseIAM.GetAsBool(), false)

		assert.Equal(t, Params.IAMEndpoint.GetAsString(), "")

		t.Logf("Minio BucketName = %s", Params.BucketName.GetAsString())

		t.Logf("Minio rootpath = %s", Params.RootPath.GetAsString())
	})
}
