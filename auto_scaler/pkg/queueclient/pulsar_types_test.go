package queueclient

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPulsarTopicStats(t *testing.T) {
	jsonData := []byte(`
	{
		"msgRateIn":0.9279094700845726,
		"msgThroughputIn":57.29246164009361,
		"msgRateOut":0.0,
		"msgThroughputOut":0.0,
		"bytesInCounter":6118,
		"msgInCounter":99,
		"bytesOutCounter":0,
		"msgOutCounter":0,
		"averageMsgSize":61.743589743589745,
		"msgChunkPublished":false,
		"storageSize":6176,
		"backlogSize":6176,
		"publishers":[
		   {
			  "msgRateIn":0.9279094700845726,
			  "msgThroughputIn":57.29246164009361,
			  "averageMsgSize":61.0,
			  "chunkedMessageRate":0.0,
			  "producerId":1,
			  "metadata":{
				 
			  },
			  "address":"/10.240.1.52:44334",
			  "connectedSince":"2020-11-05T10:20:53.677Z",
			  "producerName":"pulsar-dev-21-1"
		   }
		],
		"subscriptions":{
		   "my-sub":{
			  "msgRateOut":0.0,
			  "msgThroughputOut":0.0,
			  "bytesOutCounter":0,
			  "msgOutCounter":0,
			  "msgRateRedeliver":0.0,
			  "chuckedMessageRate":0,
			  "msgBacklog":100,
			  "msgBacklogNoDelayed":100,
			  "blockedSubscriptionOnUnackedMsgs":false,
			  "msgDelayed":0,
			  "unackedMessages":0,
			  "msgRateExpired":0.0,
			  "lastExpireTimestamp":0,
			  "lastConsumedFlowTimestamp":0,
			  "lastConsumedTimestamp":0,
			  "lastAckedTimestamp":0,
			  "consumers":[
				 
			  ],
			  "isDurable":true,
			  "isReplicated":false
		   }
		},
		"replication":{
		   
		},
		"deduplicationStatus":"Disabled"
	 }
	`)
	s := &PulsarTopicStats{}
	err := json.Unmarshal(jsonData, s)
	assert.NoError(t, err)
	value, ok := s.Subscriptions["noexist"]
	assert.False(t, ok)
	value, ok = s.Subscriptions["my-sub"]
	assert.True(t, ok)
	assert.Equal(t, int32(100), value.MsgBacklog)

}
