package config

import "fmt"

var (
	// SinkerReleaseVersion information.
	SinkerReleaseVersion = "None"
	SinkerEdition        = "None"
	SinkerGitHash        = "None"
	SinkerGitBranch      = "None"
	SinkerBuildTS        = "None"
)

func GetSinkerInfo() string {
	return fmt.Sprintf("Release Version: %s, Edition: %s, Git Commit Hash: %s, Git Branch: %s, Build At: %s",
		SinkerReleaseVersion,
		SinkerEdition,
		SinkerGitHash,
		SinkerGitBranch,
		SinkerBuildTS)
}
