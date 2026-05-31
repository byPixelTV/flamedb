package util

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
)

const defaultReleaseTag = "v0.0.0"

var releaseTagPattern = regexp.MustCompile(`^v(\d+)\.(\d+)\.(\d+)$`)

// GitCommit can be injected at build time with:
// -ldflags "-X github.com/byPixelTV/flamedb/internal/util.GitCommit=<commit-hash>"
var GitCommit = "unknown"

func CurrentVersion() string {
	if version, ok := computeVersion(); ok {
		return version
	}

	if GitCommit != "" && GitCommit != "unknown" {
		return GitCommit
	}

	if commit := buildInfoRevision(); commit != "" {
		return commit
	}

	return "local"
}

func computeVersion() (string, bool) {
	releaseTag, ok := latestReleaseTag()
	if !ok {
		return "", false
	}

	major, minor, patch, ok := parseReleaseTag(releaseTag)
	if !ok {
		major, minor, patch = 0, 0, 0
	}

	releaseVersion := fmt.Sprintf("%d.%d.%d", major, minor, patch)
	if isFullReleaseBuild() {
		return releaseVersion, true
	}

	nextPatchVersion := fmt.Sprintf("%d.%d.%d", major, minor, patch+1)
	return fmt.Sprintf("%s-dev.%s", nextPatchVersion, snapshotBuildID()), true
}

func latestReleaseTag() (string, bool) {
	output, err := runGit("tag", "--list", "v[0-9]*.[0-9]*.[0-9]*", "--sort=-v:refname")
	if err != nil {
		return "", false
	}

	if output == "" {
		return defaultReleaseTag, true
	}

	for _, line := range strings.Split(output, "\n") {
		tag := strings.TrimSpace(line)
		if tag == "" {
			continue
		}
		if releaseTagPattern.MatchString(tag) {
			return tag, true
		}
	}

	return defaultReleaseTag, true
}

func parseReleaseTag(tag string) (int, int, int, bool) {
	match := releaseTagPattern.FindStringSubmatch(tag)
	if len(match) != 4 {
		return 0, 0, 0, false
	}

	major, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, 0, 0, false
	}

	minor, err := strconv.Atoi(match[2])
	if err != nil {
		return 0, 0, 0, false
	}

	patch, err := strconv.Atoi(match[3])
	if err != nil {
		return 0, 0, 0, false
	}

	return major, minor, patch, true
}

func snapshotBuildID() string {
	if value := strings.TrimSpace(os.Getenv("SNAPSHOT_BUILD")); value != "" {
		return value
	}

	if value := strings.TrimSpace(os.Getenv("GITHUB_RUN_NUMBER")); value != "" {
		return value
	}

	return "local"
}

func isFullReleaseBuild() bool {
	if release, ok := envBool("RELEASE"); ok && release {
		return true
	}

	branch, err := runGit("rev-parse", "--abbrev-ref", "HEAD")
	return err == nil && branch == "release"
}

func runGit(args ...string) (string, error) {
	output, err := exec.Command("git", args...).Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(output)), nil
}

func envBool(key string) (bool, bool) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return false, false
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, false
	}

	return parsed, true
}

func buildInfoRevision() string {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}

	for _, setting := range buildInfo.Settings {
		if setting.Key == "vcs.revision" && setting.Value != "" {
			return setting.Value
		}
	}

	return ""
}
