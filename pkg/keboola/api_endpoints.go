package keboola

// api_endpoints.go contains definitions of all API endpoints used in Keboola Go Client.
// Centralizing endpoints allows easier maintenance and updates if API addresses change.

const (
	// Storage API endpoints.
	StorageAPIBranch      = "branches/{branchId}"
	StorageAPIBranches    = "branches"
	StorageAPIConfig      = "components/{componentId}/configs/{configId}"
	StorageAPIConfigs     = "components/{componentId}/configs"
	StorageAPIConfigRow   = "components/{componentId}/configs/{configId}/rows/{rowId}"
	StorageAPIConfigRows  = "components/{componentId}/configs/{configId}/rows"
	StorageAPIFilesUpload = "files/prepare"
	StorageAPIFile        = "files/{fileId}"
	StorageAPIFiles       = "files"
	StorageAPITableData   = "tables/{tableId}/data"
	StorageAPITable       = "tables/{tableId}"
	StorageAPITables      = "tables"
	StorageAPITickets     = "tickets"
	StorageAPIToken       = "tokens/{tokenId}" //nolint:gosec // This is a URL pattern, not a credential
	StorageAPITokens      = "tokens"
	StorageAPITokenVerify = "tokens/verify"
	StorageAPIComponents  = "components"

	// Queue API endpoints.
	QueueAPIJobs = "jobs"
	QueueAPIJob  = "jobs/{jobId}"

	// Encryption API endpoints.
	EncryptionAPIEncrypt = "encrypt"

	// Scheduler API endpoints.
	SchedulerAPISchedules       = "schedules"
	SchedulerAPISchedule        = "schedules/{scheduleId}"
	SchedulerAPIConfigSchedules = "configurations/{configurationId}"
	SchedulerAPIRefreshToken    = "schedules/{scheduleId}/refreshToken"

	// Workspaces (Sandboxes) API endpoints.
	WorkspacesAPISandboxes = "sandboxes"
	WorkspacesAPISandbox   = "sandboxes/{sandboxId}"
)
