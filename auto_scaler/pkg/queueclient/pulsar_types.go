package queueclient

type PulsarTopicStats struct {
	MsgRateIn           float32                                 `json:"msgRateIn"`
	MsgThroughputIn     float32                                 `json:"msgThroughputIn"`
	MsgRateOut          float32                                 `json:"msgRateOut"`
	MsgThroughputOut    float32                                 `json:"msgThroughputOut"`
	BytesInCounter      int32                                   `json:"bytesInCounter"`
	MsgInCounter        int32                                   `json:"msgInCounter"`
	BytesOutCounter     int32                                   `json:"bytesOutCounter"`
	MsgOutCounter       int32                                   `json:"msgOutCounter"`
	AverageMsgSize      float32                                 `json:"averageMsgSize"`
	MsgChunkPublished   bool                                    `json:"msgChunkPublished"`
	StorageSize         int32                                   `json:"storageSize"`
	BacklogSize         int32                                   `json:"backlogSize"`
	DeduplicationStatus string                                  `json:"deduplicationStatus"`
	Publishers          []PulsarTopicPublishStats               `json:"publishers"` //TODO
	Subscriptions       map[string]PulsarTopicSubscriptionStats `json:"subscriptions"`
	Replications        []PulsarTopicReplicationStats           `json:"replications"` //TODO

}

type PulsarTopicPublishStats struct {
}

type PulsarTopicSubscriptionStats struct {
	MsgBacklog      int32 `json:"msgBacklog"`
	UnackedMessages int32 `json:unackedMessages`
}

type PulsarTopicReplicationStats struct {
}
