package keboola

// api_endpoints.go contains definitions of all API endpoints used in Keboola Go Client.
// Centralizing endpoints allows easier maintenance and updates if API addresses change.

const (
	// Endpoints for encryption API.
	EncryptionAPIEncrypt = "encrypt"

	// Endpoints for queue API.
	QueueAPIJobs = "jobs"
	QueueAPIJob  = "jobs/{jobId}"

	// Endpoints for scheduler API.
	SchedulerAPISchedules       = "schedules"
	SchedulerAPISchedule        = "schedules/{scheduleId}"
	SchedulerAPIConfigSchedules = "configurations/{configurationId}"
	SchedulerAPIRefreshToken    = "schedules/{scheduleId}/refreshToken"

	// Endpoints for workspaces API.
	WorkspacesAPISandboxes = "sandboxes"
	WorkspacesAPISandbox   = "sandboxes/{sandboxId}"
)
