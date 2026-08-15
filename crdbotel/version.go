package crdbotel

import (
	"runtime/debug"
	"sync"
)

var instrumentationVersion = sync.OnceValue(func() string {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if buildInfo.Main.Path == instrumentationName {
		return buildInfo.Main.Version
	}
	for _, dependency := range buildInfo.Deps {
		if dependency.Path == instrumentationName {
			return dependency.Version
		}
	}
	return "unknown"
})
