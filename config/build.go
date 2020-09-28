package config

import "fmt"

var (
	// SinkerReleaseVersion information.
	SinkerReleaseVersion = "None"
	SinkerBuildTS        = "None"
	SinkerGitHash        = "None"
	SinkerGitBranch      = "None"
	SinkerEdition        = "None"
)

func PrintSinkerInfo() {
	fmt.Println("Release Version:", SinkerReleaseVersion)
	fmt.Println("Edition:", SinkerEdition)
	fmt.Println("Git Commit Hash:", SinkerGitHash)
	fmt.Println("Git Branch:", SinkerGitBranch)
	fmt.Println("UTC Build Time: ", SinkerBuildTS)
}
