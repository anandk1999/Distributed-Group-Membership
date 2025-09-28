package common

// DefaultPort is the TCP port used by the distributed log querier server.
const DefaultPort = 10081

// DefaultFileType is the logical key a client can use when they want to read
// the primary MP2 membership log (node.log).
const DefaultFileType = "node"

// ServerRequest is the payload sent from a client to a log server when issuing
// a query.
type ServerRequest struct {
	Input    string `json:"input"`
	FileType string `json:"file_type"`
}

// ServerResponse is the per-line payload streamed back by the server.
type ServerResponse struct {
	Output  string `json:"output"`
	LogFile string `json:"log_file"`
}
