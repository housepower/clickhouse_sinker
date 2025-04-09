package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvert(t *testing.T) {
	var hosts = [][]string{
		{"127.0.0.1:8080", "127.0.0.1:8081"},
		{"127.0.0.1:8082", "127.0.0.1:8083"},
	}

	shards := hosts2shards(hosts)
	target := Shards{
		Replicas{"127.0.0.1:8080", "127.0.0.1:8081"},
		Replicas{"127.0.0.1:8082", "127.0.0.1:8083"},
	}
	assert.Equal(t, target, shards)
}

func TestDiffShards(t *testing.T) {
	var old1 = Shards{
		Replicas{"127.0.0.1:8080", "127.0.0.1:8081"},
		Replicas{"127.0.0.1:8082", "127.0.0.1:8083"},
	}
	var new1 = Shards{
		Replicas{"127.0.0.1:8080", "127.0.0.1:8081"},
		Replicas{"127.0.0.1:8082", "127.0.0.1:8083"},
		Replicas{"127.0.0.1:8084", "127.0.0.1:8085"},
	}

	assert.Equal(t, true, diffShards(old1, new1))

	var new2 = Shards{
		Replicas{"127.0.0.1:8082", "127.0.0.1:8083"},
		Replicas{"127.0.0.1:8081", "127.0.0.1:8080"},
	}
	assert.Equal(t, true, diffShards(old1, new2))

	var new3 = Shards{
		Replicas{"127.0.0.1:8082", "127.0.0.1:8083"},
		Replicas{"127.0.0.2:8081", "127.0.0.1:8080"},
	}
	assert.Equal(t, true, diffShards(old1, new3))

	var new4 = Shards{
		Replicas{"127.0.0.1:8081", "127.0.0.1:8080"},
		Replicas{"127.0.0.1:8083", "127.0.0.1:8082"},
	}
	assert.Equal(t, false, diffShards(old1, new4))
}
