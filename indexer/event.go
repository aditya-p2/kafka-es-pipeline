package main

type Event struct {
	Before interface{} `json:"before"` // This can be nil or any other type depending on the use case
	After  After       `json:"after"`
	Op     string      `json:"op"`
	TsMs   int64       `json:"ts_ms"`
}

type After struct {
	Key   string `json:"key"`
	Value *Value `json:"value"`
}

type Value struct {
	Type   int     `json:"type"`
	Object *Object `json:"object"`
}

type Object struct {
	ID              string           `json:"id"`
	Type            string           `json:"type"`
	Labels          Labels           `json:"labels"`
	Version         string           `json:"version"`
	Hostname        string           `json:"hostname"`
	LastPing        int64            `json:"last_ping"`
	CreatedAt       int64            `json:"created_at"`
	UpdatedAt       int64            `json:"updated_at"`
	ConfigHash      string           `json:"config_hash"`
	ProcessConf     *ProcessConf     `json:"process_conf"`
	ConnectionState *ConnectionState `json:"connection_state"`
	DataPlaneCertID string           `json:"data_plane_cert_id"`
}

type Labels struct {
	Region    string `json:"region"`
	Provider  string `json:"provider"`
	ManagedBy string `json:"managed-by"`
	NetworkID string `json:"network-id"`
	DpGroupID string `json:"dp-group-id"`
}

type ProcessConf struct {
	Plugins           []string `json:"plugins"`
	LmdbMapSize       string   `json:"lmdb_map_size"`
	RouterFlavor      string   `json:"router_flavor"`
	ClusterMaxPayload int      `json:"cluster_max_payload"`
}

type ConnectionState struct {
	IsConnected bool `json:"is_connected"`
}
