load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "issue15006.s",
        "monotime.go",
        "nanotime.go",
    ],
    importmap = "k8s.io/kubernetes/vendor/github.com/coreos/etcd/pkg/monotime",
    importpath = "github.com/coreos/etcd/pkg/monotime",
    visibility = ["//visibility:public"],
)

filegroup(
    name = "package-srcs",
    srcs = glob(["**"]),
    tags = ["automanaged"],
    visibility = ["//visibility:private"],
)

filegroup(
    name = "all-srcs",
    srcs = [":package-srcs"],
    tags = ["automanaged"],
    visibility = ["//visibility:public"],
)
